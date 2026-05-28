//! [`waymark_vm_runtime_exception`] trait implementations.

use waymark_vm_runtime_exception::{
    AsException, Exception, IntoException, NotAnExceptionError, NotAnOwnedExceptionError,
};

use crate::PromiseValue;

impl<T> AsException for PromiseValue<T>
where
    T: AsException,
{
    fn as_exception(&self) -> Result<&Exception<Self::RootValue>, NotAnExceptionError> {
        let value = self.require_ready_ref().map_err(|_| NotAnExceptionError)?;
        value.as_exception()
    }
}

impl<T> IntoException for PromiseValue<T>
where
    T: IntoException,
{
    fn into_exception(self) -> Result<Exception<Self::RootValue>, NotAnOwnedExceptionError<Self>> {
        match self {
            Self::Ready(value) => value
                .into_exception()
                .map_err(|err| NotAnOwnedExceptionError {
                    value: Self::Ready(err.value),
                }),
            Self::Pending(promise_state_id) => Err(NotAnOwnedExceptionError {
                value: Self::Pending(promise_state_id),
            }),
        }
    }
}
