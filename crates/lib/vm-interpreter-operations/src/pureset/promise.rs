//! The promise-level implementations.
//!
//! Every operation here requires a ready operand; the scalar gate is the
//! single place arithmetic crosses the promise boundary, which is why the
//! arithmetic traits themselves have no promise-level form.

use waymark_vm_interpreter_pureset::operations::{
    AsDictKey, AsExceptionTypeId, AsExceptionTypeIdError, AsScalarValue, CaptureCopy, DotOp,
    IndexOp, Length, ListAppend, ListAppendError, LoadConst, MakeDict, MakeDictError, MakeList,
    MakeListError,
};
use waymark_vm_runtime_promise_value::PromiseValue;

use crate::Operations;
use crate::promise::{MaybeUnresolvedError, UnresolvedOperandError};

impl<Variation, T> CaptureCopy<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: CaptureCopy<T>,
{
    fn capture_copy(value: &PromiseValue<T>) -> PromiseValue<T> {
        match value {
            PromiseValue::Ready(value) => PromiseValue::Ready(
                <Operations<Variation> as CaptureCopy<T>>::capture_copy(value),
            ),
            PromiseValue::Pending(promise_state_id) => PromiseValue::Pending(*promise_state_id),
        }
    }
}

impl<Variation, T, ConstValue> LoadConst<PromiseValue<T>, ConstValue> for Operations<Variation>
where
    Operations<Variation>: LoadConst<T, ConstValue>,
{
    fn load_const(const_value: ConstValue) -> PromiseValue<T> {
        PromiseValue::Ready(
            <Operations<Variation> as LoadConst<T, ConstValue>>::load_const(const_value),
        )
    }
}

impl<Variation, T> AsScalarValue<PromiseValue<T>> for Operations<Variation> {
    type ScalarValue = T;
    type Error = UnresolvedOperandError;

    fn as_scalar_value(value: &PromiseValue<T>) -> Result<&Self::ScalarValue, Self::Error> {
        value.require_ready_ref().map_err(UnresolvedOperandError)
    }

    fn from_scalar_value(scalar: Self::ScalarValue) -> PromiseValue<T> {
        PromiseValue::Ready(scalar)
    }
}

impl<Variation, T> MakeList<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: MakeList<T>,
    T: waymark_vm_runtime_value::RootValueAccess,
{
    fn make_list<I>(items: I) -> Result<PromiseValue<T>, MakeListError>
    where
        I: IntoIterator<Item = T::RootValue>,
    {
        Ok(PromiseValue::Ready(<Operations<Variation> as MakeList<
            T,
        >>::make_list(items)?))
    }
}

impl<Variation, T> ListAppend<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: ListAppend<T>,
    T: waymark_vm_runtime_value::RootValueAccess,
{
    fn list_append(
        list: &PromiseValue<T>,
        item: T::RootValue,
    ) -> Result<PromiseValue<T>, ListAppendError> {
        let list = list
            .require_ready_ref()
            .map_err(|_| ListAppendError::NotListable)?;
        Ok(PromiseValue::Ready(
            <Operations<Variation> as ListAppend<T>>::list_append(list, item)?,
        ))
    }
}

impl<Variation, T> MakeDict<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: MakeDict<T>,
    T: waymark_vm_runtime_value::RootValueAccess,
{
    fn make_dict<I>(entries: I) -> Result<PromiseValue<T>, MakeDictError>
    where
        I: IntoIterator<Item = (String, T::RootValue)>,
    {
        Ok(PromiseValue::Ready(<Operations<Variation> as MakeDict<
            T,
        >>::make_dict(entries)?))
    }
}

impl<Variation, T> AsDictKey<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: AsDictKey<T>,
{
    type Error = MaybeUnresolvedError<<Operations<Variation> as AsDictKey<T>>::Error>;

    fn as_dict_key(value: &PromiseValue<T>) -> Result<&str, Self::Error> {
        let value = value
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;
        <Operations<Variation> as AsDictKey<T>>::as_dict_key(value)
            .map_err(MaybeUnresolvedError::Ready)
    }
}

impl<Variation, T> AsExceptionTypeId<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: AsExceptionTypeId<T>,
{
    fn as_exception_type_id(value: &PromiseValue<T>) -> Result<&str, AsExceptionTypeIdError> {
        let value = value
            .require_ready_ref()
            .map_err(|_| AsExceptionTypeIdError::UnsupportedTypeIdType)?;
        <Operations<Variation> as AsExceptionTypeId<T>>::as_exception_type_id(value)
    }
}

impl<Variation, T> waymark_vm_interpreter_pureset::operations::MakeException<PromiseValue<T>>
    for Operations<Variation>
where
    Operations<Variation>: waymark_vm_interpreter_pureset::operations::MakeException<T>,
    T: waymark_vm_runtime_value::RootValueAccess,
{
    fn make_exception(type_id: String, details: T::RootValue) -> PromiseValue<T> {
        PromiseValue::Ready(
            <Operations<Variation> as waymark_vm_interpreter_pureset::operations::MakeException<
                T,
            >>::make_exception(type_id, details),
        )
    }
}

impl<Variation, T> Length<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: Length<T>,
{
    type Length = <Operations<Variation> as Length<T>>::Length;
    type Error = MaybeUnresolvedError<<Operations<Variation> as Length<T>>::Error>;
    type FromLengthError = <Operations<Variation> as Length<T>>::FromLengthError;

    fn length(value: &PromiseValue<T>) -> Result<Self::Length, Self::Error> {
        let value = value
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;
        <Operations<Variation> as Length<T>>::length(value).map_err(MaybeUnresolvedError::Ready)
    }

    fn from_length(length: Self::Length) -> Result<PromiseValue<T>, Self::FromLengthError> {
        Ok(PromiseValue::Ready(
            <Operations<Variation> as Length<T>>::from_length(length)?,
        ))
    }
}

impl<Variation, T> IndexOp<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: IndexOp<T>,
    T: waymark_vm_runtime_value::RootValueAccess,
{
    type Error = MaybeUnresolvedError<<Operations<Variation> as IndexOp<T>>::Error>;

    fn index(
        object: &PromiseValue<T>,
        index: &PromiseValue<T>,
    ) -> Result<T::RootValue, Self::Error> {
        let object = object
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;
        let index = index
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;

        <Operations<Variation> as IndexOp<T>>::index(object, index)
            .map_err(MaybeUnresolvedError::Ready)
    }
}

impl<Variation, T> DotOp<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: DotOp<T>,
    T: waymark_vm_runtime_value::RootValueAccess,
{
    type Error = MaybeUnresolvedError<<Operations<Variation> as DotOp<T>>::Error>;

    fn dot(object: &PromiseValue<T>, attribute: &str) -> Result<T::RootValue, Self::Error> {
        let object = object
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;

        <Operations<Variation> as DotOp<T>>::dot(object, attribute)
            .map_err(MaybeUnresolvedError::Ready)
    }
}
