//! The VM-stopped hook and the error type it is declared over.

use typle::typle;

/// Carries the type of the error a hook observes when a driver run stops.
pub trait HasError {
    /// The type of the error the driver run returns.
    type Error;
}

/// Observes the end of a driver run.
pub trait VmStopped: HasError {
    /// The driver run has stopped.
    ///
    /// Called once, with the error the run returned — the only way a run
    /// ends.
    ///
    /// An implementation names the driver's error type as its
    /// [`Error`](HasError::Error) and may tell the exits apart by it; the
    /// driver itself only requires the type to be its own.
    fn vm_stopped(&self, error: &Self::Error);
}

/// A tuple of hooks is declared over the error type its components agree
/// on.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Error> HasError for T
where
    T<_>: HasError<Error = Error>,
{
    type Error = Error;
}

/// A tuple of hooks observes as each of its components in turn, first to
/// last.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Error> VmStopped for T
where
    T<_>: VmStopped<Error = Error>,
{
    fn vm_stopped(&self, error: &Self::Error) {
        for typle_index!(i) in 0..T::LEN {
            self[[i]].vm_stopped(error);
        }
    }
}

/// An optional hook is declared over its hook's error type.
impl<Hooks> HasError for Option<Hooks>
where
    Hooks: HasError,
{
    type Error = Hooks::Error;
}

/// An optional hook observes when present and not at all when absent.
impl<Hooks> VmStopped for Option<Hooks>
where
    Hooks: VmStopped,
{
    fn vm_stopped(&self, error: &Self::Error) {
        if let Some(hooks) = self {
            hooks.vm_stopped(error);
        }
    }
}
