//! [`waymark_vm_interpreter_excset`] trait implementations.

use crate::PromiseValue;

impl<T> waymark_vm_interpreter_excset::value::AsException for PromiseValue<T>
where
    T: waymark_vm_interpreter_excset::value::AsException,
{
    fn as_exception(
        &self,
    ) -> Result<
        &waymark_vm_runtime_exception::Exception<Self::RootValue>,
        waymark_vm_interpreter_excset::value::AsExceptionError,
    > {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_excset::value::AsExceptionError::NotAnException)?;
        value.as_exception()
    }
}

impl<T> waymark_vm_interpreter_excset::value::AsExceptionTypeId for PromiseValue<T>
where
    T: waymark_vm_interpreter_excset::value::AsExceptionTypeId,
{
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_excset::value::AsExceptionTypeIdError> {
        let value = self.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_excset::value::AsExceptionTypeIdError::NotAnExceptionTypeId
        })?;
        value.as_exception_type_id()
    }
}

impl<T> waymark_vm_interpreter_excset::value::FromIsException for PromiseValue<T>
where
    T: waymark_vm_interpreter_excset::value::FromIsException,
{
    fn from_is_exception(is_exception: bool) -> Self::RootValue {
        T::from_is_exception(is_exception)
    }
}

impl<T> waymark_vm_interpreter_excset::value::CaptureExceptionDetails for PromiseValue<T>
where
    T: waymark_vm_interpreter_excset::value::CaptureExceptionDetails,
{
    fn from_exception_details(value: &Self::RootValue) -> Self::RootValue {
        T::from_exception_details(value)
    }
}
