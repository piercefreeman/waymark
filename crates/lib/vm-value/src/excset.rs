//! [`waymark_vm_interpreter_excset`] trait implementations for [`ReadyValue`].

use crate::ReadyValue;

#[cfg(test)]
static_assertions::assert_impl_all!(crate::Value: waymark_vm_interpreter_excset::Value);

impl waymark_vm_runtime_exception::AsException for ReadyValue {
    fn as_exception(
        &self,
    ) -> Result<
        &waymark_vm_runtime_exception::Exception<Self::RootValue>,
        waymark_vm_runtime_exception::NotAnExceptionError,
    > {
        match self {
            Self::Exception(exception) => Ok(exception.as_ref()),
            Self::Int(_)
            | Self::Float(_)
            | Self::Bool(_)
            | Self::String(_)
            | Self::None
            | Self::List(_)
            | Self::Dict(_) => Err(waymark_vm_runtime_exception::NotAnExceptionError),
        }
    }
}

impl waymark_vm_interpreter_excset::value::AsExceptionTypeId for ReadyValue {
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError> {
        match self {
            Self::String(value) => Ok(value),
            Self::Int(_)
            | Self::Float(_)
            | Self::Bool(_)
            | Self::None
            | Self::Exception(_)
            | Self::List(_)
            | Self::Dict(_) => Err(
                waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError,
            ),
        }
    }
}

impl waymark_vm_interpreter_excset::value::FromIsException for ReadyValue {
    fn from_is_exception(is_exception: bool) -> Self::RootValue {
        crate::Value::Ready(Self::Bool(is_exception))
    }
}

impl waymark_vm_interpreter_excset::value::CaptureExceptionDetails for ReadyValue {
    fn from_exception_details(value: &Self::RootValue) -> Self::RootValue {
        value.clone()
    }
}
