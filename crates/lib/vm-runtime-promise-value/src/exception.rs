//! [`waymark_vm_runtime_exception`] trait implementations.

use waymark_vm_runtime_promise_core::UnresolvedPromiseError;

use crate::PromiseValue;

/// Why a promise value cannot be raised.
#[derive(Debug, thiserror::Error)]
pub enum ValueToRaisedExceptionError<ReadyValueError> {
    /// A pending promise is not an exception.
    #[error("a pending promise cannot be raised: {0}")]
    Pending(#[source] UnresolvedPromiseError),

    /// The ready value cannot be raised.
    #[error("ready value: {0}")]
    Ready(#[source] ReadyValueError),
}

impl<T, RaisedException> waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>
    for PromiseValue<T>
where
    T: waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>,
{
    type Error = ValueToRaisedExceptionError<T::Error>;

    fn into_raised(self) -> Result<RaisedException, Self::Error> {
        match self {
            Self::Ready(value) => value
                .into_raised()
                .map_err(ValueToRaisedExceptionError::Ready),
            Self::Pending(promise_state_id) => Err(ValueToRaisedExceptionError::Pending(
                UnresolvedPromiseError { promise_state_id },
            )),
        }
    }
}

impl<T, RaisedException> waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>
    for PromiseValue<T>
where
    T: waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>,
{
    type Error = T::Error;

    fn from_raised(raised: RaisedException) -> Result<Self, Self::Error> {
        T::from_raised(raised).map(Self::Ready)
    }
}
