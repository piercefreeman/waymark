//! [`waymark_vm_interpreter_pureset`] operations implementations.
//!
//! Holds the two kinds of implementation that are written once for every
//! variation: the *provided* vocabulary (mechanical construction and
//! capture over the shared value shape) and the *promise-level* forms of
//! the whole vocabulary, which require a ready operand and delegate to
//! the ready-level implementation.

use waymark_vm_interpreter_pureset::operations::{
    AsDictKey, AsExceptionTypeId, AsExceptionTypeIdError, AsScalarValue, BinaryOps, CaptureCopy,
    DotOp, IndexOp, Length, ListAppend, ListAppendError, LoadConst, MakeDict, MakeDictError,
    MakeList, MakeListError, UnaryOps,
};
use waymark_vm_runtime_promise_value::PromiseValue;
use waymark_vm_value::{ReadyValue, Value};

use crate::Operations;
use crate::promise::{MaybeUnresolvedError, UnresolvedOperandError};

// --- Provided vocabulary, ready level ---

impl<Variation> CaptureCopy<ReadyValue> for Operations<Variation> {
    fn capture_copy(value: &ReadyValue) -> ReadyValue {
        value.clone()
    }
}

impl<Variation> MakeList<ReadyValue> for Operations<Variation> {
    fn make_list<I>(items: I) -> Result<ReadyValue, MakeListError>
    where
        I: IntoIterator<Item = Value>,
    {
        Ok(ReadyValue::List(items.into_iter().collect()))
    }
}

impl<Variation> ListAppend<ReadyValue> for Operations<Variation> {
    fn list_append(list: &ReadyValue, item: Value) -> Result<ReadyValue, ListAppendError> {
        let ReadyValue::List(existing) = list else {
            return Err(ListAppendError::NotListable);
        };
        let mut grown = Vec::with_capacity(existing.len() + 1);
        grown.extend(existing.iter().cloned());
        grown.push(item);
        Ok(ReadyValue::List(grown))
    }
}

impl<Variation> MakeDict<ReadyValue> for Operations<Variation> {
    fn make_dict<I>(entries: I) -> Result<ReadyValue, MakeDictError>
    where
        I: IntoIterator<Item = (String, Value)>,
    {
        let mut dict = indexmap::IndexMap::new();

        for (key, value) in entries {
            dict.insert(key, value);
        }

        Ok(ReadyValue::Dict(dict))
    }
}

impl<Variation> AsExceptionTypeId<ReadyValue> for Operations<Variation> {
    fn as_exception_type_id(value: &ReadyValue) -> Result<&str, AsExceptionTypeIdError> {
        match value {
            ReadyValue::String(value) => Ok(value),
            ReadyValue::Int(_)
            | ReadyValue::Float(_)
            | ReadyValue::Bool(_)
            | ReadyValue::None
            | ReadyValue::List(_)
            | ReadyValue::Dict(_)
            | ReadyValue::Exception(_) => Err(AsExceptionTypeIdError::UnsupportedTypeIdType),
        }
    }
}

impl<Variation> waymark_vm_interpreter_pureset::operations::MakeException<ReadyValue>
    for Operations<Variation>
{
    fn make_exception(type_id: String, details: Value) -> ReadyValue {
        ReadyValue::Exception(Box::new(waymark_vm_runtime_exception::Exception {
            type_id,
            details,
        }))
    }
}

impl<Variation> waymark_vm_runtime_exception::ExceptionFromIntermediate<String, ReadyValue>
    for Operations<Variation>
{
    fn from_intermediate_exception(
        exception: waymark_vm_runtime_exception::Exception<String>,
    ) -> waymark_vm_runtime_exception::Exception<Value> {
        waymark_vm_runtime_exception::Exception {
            type_id: exception.type_id,
            details: Value::Ready(ReadyValue::String(exception.details)),
        }
    }
}

// --- Semantic vocabulary: delegated to the variation ---

impl<Variation, ConstValue> LoadConst<ReadyValue, ConstValue> for Operations<Variation>
where
    Variation: LoadConst<ReadyValue, ConstValue>,
{
    fn load_const(const_value: ConstValue) -> ReadyValue {
        <Variation as LoadConst<ReadyValue, ConstValue>>::load_const(const_value)
    }
}

impl<Variation> BinaryOps<ReadyValue> for Operations<Variation>
where
    Variation: BinaryOps<ReadyValue>,
{
    type Error = <Variation as BinaryOps<ReadyValue>>::Error;

    fn add(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::add(a, b)
    }

    fn sub(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::sub(a, b)
    }

    fn mul(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::mul(a, b)
    }

    fn div(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::div(a, b)
    }

    fn floor_div(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::floor_div(a, b)
    }

    fn modulo(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::modulo(a, b)
    }

    fn eq(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::eq(a, b)
    }

    fn ne(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::ne(a, b)
    }

    fn lt(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::lt(a, b)
    }

    fn le(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::le(a, b)
    }

    fn gt(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::gt(a, b)
    }

    fn ge(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::ge(a, b)
    }

    fn contains(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::contains(a, b)
    }

    fn not_contains(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::not_contains(a, b)
    }

    fn and(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::and(a, b)
    }

    fn or(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as BinaryOps<ReadyValue>>::or(a, b)
    }
}

impl<Variation> UnaryOps<ReadyValue> for Operations<Variation>
where
    Variation: UnaryOps<ReadyValue>,
{
    type Error = <Variation as UnaryOps<ReadyValue>>::Error;

    fn neg(value: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as UnaryOps<ReadyValue>>::neg(value)
    }

    fn not(value: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        <Variation as UnaryOps<ReadyValue>>::not(value)
    }
}

impl<Variation> AsDictKey<ReadyValue> for Operations<Variation>
where
    Variation: AsDictKey<ReadyValue>,
{
    type Error = <Variation as AsDictKey<ReadyValue>>::Error;

    fn as_dict_key(value: &ReadyValue) -> Result<&str, Self::Error> {
        <Variation as AsDictKey<ReadyValue>>::as_dict_key(value)
    }
}

impl<Variation> Length<ReadyValue> for Operations<Variation>
where
    Variation: Length<ReadyValue>,
{
    type Length = <Variation as Length<ReadyValue>>::Length;
    type Error = <Variation as Length<ReadyValue>>::Error;
    type FromLengthError = <Variation as Length<ReadyValue>>::FromLengthError;

    fn length(value: &ReadyValue) -> Result<Self::Length, Self::Error> {
        <Variation as Length<ReadyValue>>::length(value)
    }

    fn from_length(length: Self::Length) -> Result<ReadyValue, Self::FromLengthError> {
        <Variation as Length<ReadyValue>>::from_length(length)
    }
}

impl<Variation> IndexOp<ReadyValue> for Operations<Variation>
where
    Variation: IndexOp<ReadyValue>,
{
    type Error = <Variation as IndexOp<ReadyValue>>::Error;

    fn index(object: &ReadyValue, index: &ReadyValue) -> Result<Value, Self::Error> {
        <Variation as IndexOp<ReadyValue>>::index(object, index)
    }
}

impl<Variation> DotOp<ReadyValue> for Operations<Variation>
where
    Variation: DotOp<ReadyValue>,
{
    type Error = <Variation as DotOp<ReadyValue>>::Error;

    fn dot(object: &ReadyValue, attribute: &str) -> Result<Value, Self::Error> {
        <Variation as DotOp<ReadyValue>>::dot(object, attribute)
    }
}

// --- Promise level ---
//
// Every operation below requires a ready operand; the scalar gate is the
// single place arithmetic crosses the promise boundary, which is why the
// arithmetic traits themselves have no promise-level form.

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
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    fn make_list<I>(items: I) -> Result<PromiseValue<T>, MakeListError>
    where
        I: IntoIterator<Item = PromiseValue<T>>,
    {
        Ok(PromiseValue::Ready(<Operations<Variation> as MakeList<
            T,
        >>::make_list(items)?))
    }
}

impl<Variation, T> ListAppend<PromiseValue<T>> for Operations<Variation>
where
    Operations<Variation>: ListAppend<T>,
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    fn list_append(
        list: &PromiseValue<T>,
        item: PromiseValue<T>,
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
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    fn make_dict<I>(entries: I) -> Result<PromiseValue<T>, MakeDictError>
    where
        I: IntoIterator<Item = (String, PromiseValue<T>)>,
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
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    fn make_exception(type_id: String, details: PromiseValue<T>) -> PromiseValue<T> {
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
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    type Error = MaybeUnresolvedError<<Operations<Variation> as IndexOp<T>>::Error>;

    fn index(
        object: &PromiseValue<T>,
        index: &PromiseValue<T>,
    ) -> Result<PromiseValue<T>, Self::Error> {
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
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    type Error = MaybeUnresolvedError<<Operations<Variation> as DotOp<T>>::Error>;

    fn dot(object: &PromiseValue<T>, attribute: &str) -> Result<PromiseValue<T>, Self::Error> {
        let object = object
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;

        <Operations<Variation> as DotOp<T>>::dot(object, attribute)
            .map_err(MaybeUnresolvedError::Ready)
    }
}

impl<Variation, T, IntermediateDetails>
    waymark_vm_runtime_exception::ExceptionFromIntermediate<IntermediateDetails, PromiseValue<T>>
    for Operations<Variation>
where
    Operations<Variation>:
        waymark_vm_runtime_exception::ExceptionFromIntermediate<IntermediateDetails, T>,
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = PromiseValue<T>>,
{
    fn from_intermediate_exception(
        exception: waymark_vm_runtime_exception::Exception<IntermediateDetails>,
    ) -> waymark_vm_runtime_exception::Exception<PromiseValue<T>> {
        <Operations<Variation> as waymark_vm_runtime_exception::ExceptionFromIntermediate<
            IntermediateDetails,
            T,
        >>::from_intermediate_exception(exception)
    }
}
