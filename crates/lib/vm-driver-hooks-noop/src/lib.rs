//! VM driver hooks that observe nothing.

#![warn(missing_docs)]

use derive_where::derive_where;
use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Hooks that observe nothing.
///
/// Implements every hook as a no-op, for the runs that have no observer.
/// The type parameters only pin the effect, value and error types the hooks
/// are declared over; `Noop` is `Send`, `Sync` and `'static` regardless of
/// them.
#[derive_where(Debug, Default)]
pub struct Noop<Effect, Value, Error>(core::marker::PhantomData<Parameters<Effect, Value, Error>>);

/// The hooks' type parameters, held as a function pointer type so that
/// [`Noop`] stays `Send`, `Sync` and `'static` whatever they are.
type Parameters<Effect, Value, Error> = fn() -> (Effect, Value, Error);

impl<Effect, Value, Error> Noop<Effect, Value, Error> {
    /// Create the no-op hooks.
    pub const fn new() -> Self {
        Self(core::marker::PhantomData)
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::effect_emitted::HasEffect
    for Noop<Effect, Value, Error>
{
    type Effect = Effect;
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::promise_settled::HasValue
    for Noop<Effect, Value, Error>
{
    type Value = Value;
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::vm_stopped::HasError
    for Noop<Effect, Value, Error>
{
    type Error = Error;
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::VmStarted for Noop<Effect, Value, Error> {
    fn vm_started(&self) {}
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::EffectEmitted for Noop<Effect, Value, Error> {
    fn effect_emitted(&self, _number: EffectNumber, _effect: &Self::Effect) {}
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::PromiseSettled for Noop<Effect, Value, Error> {
    fn promise_settled(
        &self,
        _promise_state_id: PromiseStateId,
        _resolution: &PromiseResolution<Self::Value>,
    ) {
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::SnapshotPersisted
    for Noop<Effect, Value, Error>
{
    fn snapshot_persisted(&self, _size_in_bytes: usize) {}
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::VmStopped for Noop<Effect, Value, Error> {
    fn vm_stopped(&self, _error: &Self::Error) {}
}

#[cfg(test)]
mod tests {
    use super::Noop;

    fn assert_send_sync_static<T: Send + Sync + 'static>(_value: &T) {}

    #[test]
    fn noop_is_thread_safe_regardless_of_its_parameters() {
        let noop = Noop::<std::rc::Rc<()>, std::rc::Rc<()>, std::rc::Rc<()>>::new();

        assert_send_sync_static(&noop);
    }

    #[test]
    fn noop_default_regardless_of_its_parameters() {
        let noop = Noop::<std::rc::Rc<()>, std::rc::Rc<()>, std::rc::Rc<()>>::default();

        waymark_vm_driver_hooks::VmStarted::vm_started(&noop);
        waymark_vm_driver_hooks::SnapshotPersisted::snapshot_persisted(&noop, 0);
        waymark_vm_driver_hooks::VmStopped::vm_stopped(&noop, &std::rc::Rc::new(()));
    }
}
