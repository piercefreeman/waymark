//! [`waymark_vm_interpreter_pureset`] trait implementations.

use crate::PromiseValue;

impl<T> waymark_vm_interpreter_pureset::value::CaptureCopy for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::CaptureCopy,
{
    fn capture_copy(&self) -> Self {
        match self {
            PromiseValue::Ready(value) => PromiseValue::Ready(value.capture_copy()),
            PromiseValue::Pending(promise_state_id) => PromiseValue::Pending(*promise_state_id),
        }
    }
}

impl<T, ConstValue> waymark_vm_interpreter_pureset::value::LoadConst<ConstValue> for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::LoadConst<ConstValue>,
{
    fn load_const(const_value: ConstValue) -> Self {
        Self::Ready(T::load_const(const_value))
    }
}

impl<T> waymark_vm_interpreter_pureset::value::AsScalar for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::BinaryOps,
    T: waymark_vm_interpreter_pureset::value::UnaryOps,
{
    type Scalar = T;

    fn as_scalar(
        &self,
    ) -> Result<&Self::Scalar, waymark_vm_interpreter_pureset::value::AsScalarError> {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_pureset::value::AsScalarError::NotAScalar)?;
        Ok(value)
    }

    fn from_scalar(scalar: Self::Scalar) -> Self {
        Self::Ready(scalar)
    }
}

impl<T> waymark_vm_interpreter_pureset::value::MakeList for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::MakeList,
{
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self::RootValue>,
    {
        Ok(Self::Ready(T::make_list(items)?))
    }
}

impl<T> waymark_vm_interpreter_pureset::value::MakeDict for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::MakeDict,
{
    fn make_dict<I>(
        entries: I,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeDictError>
    where
        I: IntoIterator<Item = (String, Self::RootValue)>,
    {
        Ok(Self::Ready(T::make_dict(entries)?))
    }
}

impl<T> waymark_vm_interpreter_pureset::value::AsDictKey for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::AsDictKey,
{
    fn as_dict_key(&self) -> Result<&str, waymark_vm_interpreter_pureset::value::AsDictKeyError> {
        let value = self.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType
        })?;
        value.as_dict_key()
    }
}

impl<T> waymark_vm_interpreter_pureset::value::Length for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::Length,
{
    type Length = T::Length;

    fn length(&self) -> Result<Self::Length, waymark_vm_interpreter_pureset::value::LengthError> {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)?;
        value.length()
    }

    fn from_length(
        length: Self::Length,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::FromLengthError> {
        Ok(Self::Ready(T::from_length(length)?))
    }
}

impl<T> waymark_vm_interpreter_pureset::value::IndexOp for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::IndexOp,
{
    fn index(
        object: &Self,
        index: &Self,
    ) -> Result<Self::RootValue, waymark_vm_interpreter_pureset::value::IndexOperationError> {
        let object = object.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::IndexOperationError::UnsupportedOperation
        })?;
        let index = index.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::IndexOperationError::UnsupportedOperation
        })?;

        let value = T::index(object, index)?;

        Ok(value)
    }
}

impl<T> waymark_vm_interpreter_pureset::value::DotOp for PromiseValue<T>
where
    T: waymark_vm_interpreter_pureset::value::DotOp,
{
    fn dot(
        object: &Self,
        attribute: &str,
    ) -> Result<Self::RootValue, waymark_vm_interpreter_pureset::value::DotOperationError> {
        let object = object.require_ready_ref().map_err(|_| {
            waymark_vm_interpreter_pureset::value::DotOperationError::UnsupportedOperation
        })?;

        let value = T::dot(object, attribute)?;

        Ok(value)
    }
}
