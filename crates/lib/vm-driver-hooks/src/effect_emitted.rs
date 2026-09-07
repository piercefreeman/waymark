//! The effect-emitted hook and the effect type it is declared over.

use typle::typle;
use waymark_vm_runtime_effect::EffectNumber;

/// Carries the type of the effects a hook observes.
pub trait HasEffect {
    /// The type of the effects emitted by the runtime.
    type Effect;
}

/// Observes the effects emitted by the runtime.
pub trait EffectEmitted: HasEffect {
    /// The runtime has emitted an effect.
    ///
    /// Called before the effect is handed to the effector, with the effect
    /// number the runtime assigned to it.
    fn effect_emitted(&self, number: EffectNumber, effect: &Self::Effect);
}

/// A tuple of hooks is declared over the effect type its components agree
/// on.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Effect> HasEffect for T
where
    T<_>: HasEffect<Effect = Effect>,
{
    type Effect = Effect;
}

/// A tuple of hooks observes as each of its components in turn, first to
/// last.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Effect> EffectEmitted for T
where
    T<_>: EffectEmitted<Effect = Effect>,
{
    fn effect_emitted(&self, number: EffectNumber, effect: &Self::Effect) {
        for typle_index!(i) in 0..T::LEN {
            self[[i]].effect_emitted(number, effect);
        }
    }
}

/// An optional hook is declared over its hook's effect type.
impl<Hooks> HasEffect for Option<Hooks>
where
    Hooks: HasEffect,
{
    type Effect = Hooks::Effect;
}

/// An optional hook observes when present and not at all when absent.
impl<Hooks> EffectEmitted for Option<Hooks>
where
    Hooks: EffectEmitted,
{
    fn effect_emitted(&self, number: EffectNumber, effect: &Self::Effect) {
        if let Some(hooks) = self {
            hooks.effect_emitted(number, effect);
        }
    }
}
