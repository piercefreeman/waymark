mod coreset;
mod exception;
mod extcallset;
mod promisable;
mod pureset;

use waymark_vm_runtime_core::PromiseStateHandle;
use waymark_vm_runtime_promise_core::UnresolvedPromiseError;

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

#[derive(Debug, thiserror::Error)]
pub enum Error<ReadyValueError> {
    #[error(transparent)]
    UnresolvedPromise(#[from] UnresolvedPromiseError),

    #[error(transparent)]
    Ready(ReadyValueError),
}

impl<T> PromiseValue<T> {
    pub fn require_ready(self) -> Result<T, (UnresolvedPromiseError, Self)> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(ref promise_state_handle) => {
                let promise_state_id = promise_state_handle.id();
                Err((UnresolvedPromiseError { promise_state_id }, self))
            }
        }
    }

    pub fn require_ready_ref(&self) -> Result<&T, UnresolvedPromiseError> {
        match self {
            PromiseValue::Ready(value) => Ok(value),
            PromiseValue::Pending(promise_state_handle) => Err(UnresolvedPromiseError {
                promise_state_id: promise_state_handle.id(),
            }),
        }
    }

    pub fn require_ready_mut(&mut self) -> Result<&mut T, UnresolvedPromiseError> {
        match self {
            PromiseValue::Ready(value) => Ok(value),
            PromiseValue::Pending(promise_state_handle) => Err(UnresolvedPromiseError {
                promise_state_id: promise_state_handle.id(),
            }),
        }
    }
}

impl<T> waymark_vm_runtime_value::RootValueAccess for PromiseValue<T>
where
    T: waymark_vm_runtime_value::RootValueAccess,
{
    type RootValue = T::RootValue;
}
