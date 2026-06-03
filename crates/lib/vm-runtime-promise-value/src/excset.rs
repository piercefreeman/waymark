//! [`waymark_vm_interpreter_excset`] trait implementations.

use crate::PromiseValue;

impl<T> waymark_vm_runtime_exception::AsException for PromiseValue<T>
where
    T: waymark_vm_runtime_exception::AsException,
{
    fn as_exception(
        &self,
    ) -> Result<
        &waymark_vm_runtime_exception::Exception<Self::RootValue>,
        waymark_vm_runtime_exception::NotAnExceptionError,
    > {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_runtime_exception::NotAnExceptionError)?;
        value.as_exception()
    }
}

impl<T> waymark_vm_runtime_exception::AsExceptionMut for PromiseValue<T>
where
    T: waymark_vm_runtime_exception::AsExceptionMut,
{
    fn as_exception_mut(
        &mut self,
    ) -> Result<
        &mut waymark_vm_runtime_exception::Exception<Self::RootValue>,
        waymark_vm_runtime_exception::NotAnExceptionError,
    > {
        let value = self
            .require_ready_mut()
            .map_err(|_| waymark_vm_runtime_exception::NotAnExceptionError)?;
        value.as_exception_mut()
    }
}

impl<T> waymark_vm_interpreter_excset::value::AsExceptionTypeId for PromiseValue<T>
where
    T: waymark_vm_interpreter_excset::value::AsExceptionTypeId,
{
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError> {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError)?;
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

impl<T> waymark_vm_interpreter_excset::value::FromShouldBubble for PromiseValue<T>
where
    T: waymark_vm_interpreter_excset::value::FromShouldBubble,
{
    fn from_should_bubble(should_bubble: bool) -> Self::RootValue {
        T::from_should_bubble(should_bubble)
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
