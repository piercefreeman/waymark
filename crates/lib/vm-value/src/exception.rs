//! [`waymark_vm_runtime_exception`] trait implementations for [`crate::Value`].

use waymark_vm_runtime_exception::{
    AsException, Exception, FromException, IntoException, NotAnExceptionError,
    NotAnOwnedExceptionError,
};

use crate::ReadyValue;

impl<Flavor: crate::Flavor> AsException for ReadyValue<Flavor> {
    fn as_exception(&self) -> Result<&Exception<Self::RootValue>, NotAnExceptionError> {
        match self {
            Self::Exception(exception) => Ok(exception.as_ref()),
            _ => Err(NotAnExceptionError),
        }
    }
}

impl<Flavor: crate::Flavor> IntoException for ReadyValue<Flavor> {
    fn into_exception(self) -> Result<Exception<Self::RootValue>, NotAnOwnedExceptionError<Self>> {
        match self {
            Self::Exception(exception) => Ok(*exception),
            value => Err(NotAnOwnedExceptionError { value }),
        }
    }
}

impl<Flavor: crate::Flavor> FromException for ReadyValue<Flavor> {
    fn from_exception(exception: Exception<Self::RootValue>) -> Self {
        Self::Exception(Box::new(exception))
    }
}

impl<Flavor: crate::Flavor> waymark_vm_runtime_exception::ExceptionFromIntermediate<String>
    for ReadyValue<Flavor>
{
    fn from_intermediate_exception(exception: Exception<String>) -> Exception<Self::RootValue> {
        Exception {
            type_id: exception.type_id,
            details: crate::Value::Ready(Self::String(exception.details)),
        }
    }
}
