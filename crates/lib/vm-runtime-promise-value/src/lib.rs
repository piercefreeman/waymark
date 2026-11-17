mod coreset;
mod exception;
mod extcallset;
mod promisable;
mod pureset;

use waymark_vm_runtime_core::PromiseStateHandle;
use waymark_vm_runtime_promise_core::{PromiseStateId, UnresolvedPromiseError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PromiseValue<T> {
    Ready(T),
    Pending(PromiseStateHandle),
}

impl<T> From<T> for PromiseValue<T> {
    fn from(value: T) -> Self {
        Self::Ready(value)
    }
}

/// Why a promise value cannot provide what an operation needs.
#[derive(Debug, thiserror::Error)]
pub enum Error<ReadyValueError> {
    /// The value is a pending promise.
    #[error("{0}")]
    UnresolvedPromise(#[source] UnresolvedPromiseError<PromiseStateId>),

    /// The ready value cannot provide it.
    #[error(transparent)]
    Ready(ReadyValueError),
}

impl<T> waymark_vm_runtime_value::RootValueAccess for PromiseValue<T>
where
    T: waymark_vm_runtime_value::RootValueAccess,
{
    type RootValue = T::RootValue;
}
