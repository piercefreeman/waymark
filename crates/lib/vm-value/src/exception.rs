//! [`waymark_vm_runtime_exception`] trait implementations for [`crate::Value`].

use waymark_vm_runtime_exception::{
    AsException, Exception, IntoException, NotAnExceptionError, NotAnOwnedExceptionError,
};

use crate::ReadyValue;

impl AsException for ReadyValue {
    fn as_exception(&self) -> Result<&Exception<Self::RootValue>, NotAnExceptionError> {
        match self {
            Self::Exception(exception) => Ok(exception.as_ref()),
            _ => Err(NotAnExceptionError),
        }
    }
}

impl IntoException for ReadyValue {
    fn into_exception(self) -> Result<Exception<Self::RootValue>, NotAnOwnedExceptionError<Self>> {
        match self {
            Self::Exception(exception) => Ok(*exception),
            value => Err(NotAnOwnedExceptionError { value }),
        }
    }
}
