//! The VM-started hook.

use typle::typle;

/// Observes the start of a driver run.
pub trait VmStarted {
    /// The driver run has started.
    ///
    /// Called once, before the runtime is stepped for the first time.
    fn vm_started(&self);
}

/// A tuple of hooks observes as each of its components in turn, first to
/// last.
#[typle(Tuple for 1..=8)]
impl<T: Tuple> VmStarted for T
where
    T<_>: VmStarted,
{
    fn vm_started(&self) {
        for typle_index!(i) in 0..T::LEN {
            self[[i]].vm_started();
        }
    }
}

/// An optional hook observes when present and not at all when absent.
impl<Hooks> VmStarted for Option<Hooks>
where
    Hooks: VmStarted,
{
    fn vm_started(&self) {
        if let Some(hooks) = self {
            hooks.vm_started();
        }
    }
}
