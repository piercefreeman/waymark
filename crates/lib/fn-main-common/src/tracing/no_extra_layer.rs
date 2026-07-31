//! The absent extra tracing layer.

/// An uninhabited layer: the `Some` type of the absent `Option` layer
/// inside [`NoExtraLayer`].
///
/// No value of this type can ever exist, so the delegation target below
/// is `None` by construction — and a downcast probe through it can never
/// yield a usable layer.
#[derive(Debug)]
enum UninhabitableLayer {}

impl<Subscriber> tracing_subscriber::Layer<Subscriber> for UninhabitableLayer where
    Subscriber: tracing::Subscriber
{
}

/// The absent extra layer, for slots with nothing to compose in; see
/// [`NO_EXTRA_LAYER`].
///
/// Composition-neutral, with the same layer behavior as the absent
/// `Option` layer it wraps and delegates to: callsite interest and
/// `enabled` stay at the permissive trait defaults — an absent layer
/// must never veto sibling layers — while the `OFF` max-level hint tells
/// the subscriber this layer enables nothing, so callsites no other
/// layer wants collapse to disabled through the global max-level gate.
/// Delegating `downcast_raw` also answers tracing-subscriber's private
/// none-layer probe (which keeps the `OFF` hint from clamping a sibling
/// layer in `and_then` pairs) without this crate naming that private
/// machinery: the inner `None` answers whatever protocol the locked
/// version expects. The inner `Option`'s `Some` type is the uninhabited
/// [`UninhabitableLayer`], so the delegate is absent by construction and
/// no downcast through it can ever produce a real layer.
#[derive(Debug, Default)]
pub struct NoExtraLayer(Option<UninhabitableLayer>);

/// The absent extra layer, for when there is nothing to compose in.
pub const NO_EXTRA_LAYER: NoExtraLayer = NoExtraLayer(None);

impl<Subscriber> tracing_subscriber::Layer<Subscriber> for NoExtraLayer
where
    Subscriber: tracing::Subscriber,
{
    fn max_level_hint(&self) -> Option<tracing_subscriber::filter::LevelFilter> {
        tracing_subscriber::Layer::<Subscriber>::max_level_hint(&self.0)
    }

    unsafe fn downcast_raw(&self, id: std::any::TypeId) -> Option<*const ()> {
        if id == std::any::TypeId::of::<Self>() {
            return Some(self as *const Self as *const ());
        }
        // SAFETY: forwarded verbatim; the inner `None` upholds the same
        // contract this method was called under.
        unsafe { tracing_subscriber::Layer::<Subscriber>::downcast_raw(&self.0, id) }
    }
}
