use waymark_vm_runtime_promise_core::UnresolvedPromiseError;

use crate::PromiseStateHandle;

/// A value type that supports promises.
///
/// A value is either ready, holding a [`Promisable::ReadyValue`], or
/// pending, holding the handle of the promise state it waits for.
pub trait Promisable {
    /// A ready value type.
    type ReadyValue;

    /// Construct a promisable value from a ready value.
    fn from_ready(value: Self::ReadyValue) -> Self;

    /// Construct a promisable value from a pending value.
    fn from_pending(promise_state_handle: PromiseStateHandle) -> Self;

    /// Whether the value is ready or pending, by reference.
    fn promisable_readiness(&self) -> PromisableReadinessRef<'_, Self::ReadyValue>;

    /// Whether the value is ready or pending, by mutable reference.
    fn promisable_readiness_mut(&mut self) -> PromisableReadinessMut<'_, Self::ReadyValue>;

    /// Whether the value is ready or pending, taking the value apart.
    fn into_promisable_readiness(self) -> PromisableReadiness<Self::ReadyValue>;
}

/// Shortcuts to a [`Promisable`] value's ready value.
///
/// Implemented for every [`Promisable`]; no other implementation is
/// possible, so the shortcuts always agree with the readiness views.
pub trait PromisableExt: Promisable {
    /// The ready value, or an error carrying the handle if the value is
    /// pending.
    fn require_ready_ref(
        &self,
    ) -> Result<&Self::ReadyValue, UnresolvedPromiseError<&PromiseStateHandle>>;

    /// The ready value by mutable reference, or an error carrying the
    /// handle if the value is pending.
    fn require_ready_mut(
        &mut self,
    ) -> Result<&mut Self::ReadyValue, UnresolvedPromiseError<&mut PromiseStateHandle>>;

    /// The ready value, or an error carrying the handle if the value is
    /// pending.
    fn require_ready(self) -> Result<Self::ReadyValue, UnresolvedPromiseError<PromiseStateHandle>>;
}

impl<T> PromisableExt for T
where
    T: Promisable,
{
    fn require_ready_ref(
        &self,
    ) -> Result<&Self::ReadyValue, UnresolvedPromiseError<&PromiseStateHandle>> {
        self.promisable_readiness().try_ready()
    }

    fn require_ready_mut(
        &mut self,
    ) -> Result<&mut Self::ReadyValue, UnresolvedPromiseError<&mut PromiseStateHandle>> {
        self.promisable_readiness_mut().try_ready()
    }

    fn require_ready(self) -> Result<Self::ReadyValue, UnresolvedPromiseError<PromiseStateHandle>> {
        self.into_promisable_readiness().try_ready()
    }
}

/// A borrowed [`Promisable`] value: ready or pending.
#[derive(Debug)]
pub enum PromisableReadinessRef<'a, ReadyValue> {
    /// The value is ready.
    Ready(&'a ReadyValue),

    /// The value waits for the promise state the handle holds.
    Pending(&'a PromiseStateHandle),
}

/// A mutably borrowed [`Promisable`] value: ready or pending.
#[derive(Debug)]
pub enum PromisableReadinessMut<'a, ReadyValue> {
    /// The value is ready.
    Ready(&'a mut ReadyValue),

    /// The value waits for the promise state the handle holds.
    Pending(&'a mut PromiseStateHandle),
}

/// A [`Promisable`] value taken apart: ready or pending.
#[derive(Debug)]
pub enum PromisableReadiness<ReadyValue> {
    /// The value is ready.
    Ready(ReadyValue),

    /// The value waits for the promise state the handle holds.
    Pending(PromiseStateHandle),
}

impl<'a, ReadyValue> PromisableReadinessRef<'a, ReadyValue> {
    /// The ready value, or an error carrying the handle if the value is
    /// pending.
    pub fn try_ready(
        self,
    ) -> Result<&'a ReadyValue, UnresolvedPromiseError<&'a PromiseStateHandle>> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(pending) => Err(UnresolvedPromiseError { pending }),
        }
    }
}

impl<'a, ReadyValue> PromisableReadinessMut<'a, ReadyValue> {
    /// The ready value, or an error carrying the handle if the value is
    /// pending.
    pub fn try_ready(
        self,
    ) -> Result<&'a mut ReadyValue, UnresolvedPromiseError<&'a mut PromiseStateHandle>> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(pending) => Err(UnresolvedPromiseError { pending }),
        }
    }
}

impl<ReadyValue> PromisableReadiness<ReadyValue> {
    /// The ready value, or an error carrying the handle if the value is
    /// pending.
    pub fn try_ready(self) -> Result<ReadyValue, UnresolvedPromiseError<PromiseStateHandle>> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(pending) => Err(UnresolvedPromiseError { pending }),
        }
    }
}
