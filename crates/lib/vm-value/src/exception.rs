//! [`waymark_vm_runtime_exception`] trait implementations for [`crate::Value`].
//!
//! The value crosses to and from the raised domain through its flavor's
//! exception value: a value raises if it is an exception that raises, and
//! captures a raised exception if the flavor's exception value does.

use crate::ReadyValue;

/// Why a ready value cannot be raised.
#[derive(Debug, thiserror::Error)]
pub enum ValueToRaisedExceptionError<ExceptionValueError> {
    /// The value is not an exception.
    #[error("the value is not an exception")]
    NotAnException,

    /// The exception value cannot be raised.
    #[error("exception value: {0}")]
    ExceptionValue(#[source] ExceptionValueError),
}

impl<Flavor, RaisedException> waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>
    for ReadyValue<Flavor>
where
    Flavor: crate::Flavor,
    Flavor::ExceptionValue: waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>,
{
    type Error = ValueToRaisedExceptionError<
        <Flavor::ExceptionValue as waymark_vm_runtime_exception::ValueToRaisedException<
            RaisedException,
        >>::Error,
    >;

    fn into_raised(self) -> Result<RaisedException, Self::Error> {
        match self {
            Self::Exception(exception) => exception
                .into_raised()
                .map_err(ValueToRaisedExceptionError::ExceptionValue),
            _ => Err(ValueToRaisedExceptionError::NotAnException),
        }
    }
}

impl<Flavor, RaisedException> waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>
    for ReadyValue<Flavor>
where
    Flavor: crate::Flavor,
    Flavor::ExceptionValue: waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>,
{
    type Error = <Flavor::ExceptionValue as waymark_vm_runtime_exception::RaisedExceptionToValue<
        RaisedException,
    >>::Error;

    fn from_raised(raised: RaisedException) -> Result<Self, Self::Error> {
        Flavor::ExceptionValue::from_raised(raised)
            .map(|exception| Self::Exception(Box::new(exception)))
    }
}
