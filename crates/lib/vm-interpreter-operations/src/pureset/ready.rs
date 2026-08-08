//! The ready-level implementations.

use waymark_vm_interpreter_pureset::operations::{
    AsDictKey, AsExceptionTypeId, AsExceptionTypeIdError, BinaryOps, CaptureCopy, DotOp, IndexOp,
    Length, ListAppend, ListAppendError, LoadConst, MakeDict, MakeDictError, MakeList,
    MakeListError, UnaryOps,
};
use waymark_vm_value::{ReadyValue, Value};

use crate::Operations;

// --- Provided vocabulary ---

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
