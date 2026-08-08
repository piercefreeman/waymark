//! [`waymark_vm_interpreter_pureset`] operations implementations for the
//! Python variation.

use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::{
    BinaryOpKind as BinaryOperationKind, UnaryOpKind as UnaryOperationKind,
};
use waymark_vm_value::{ReadyValue, ReadyValue as RV, Value};

use crate::PythonVariation;
use crate::pureset::error::{
    AsDictKeyError, BinaryOperationError, DotOperationError, FromLengthError, IndexOperationError,
    LengthError, UnaryOperationError,
};
use crate::pythonic;

fn contains_bool(a: &ReadyValue, b: &ReadyValue) -> Result<bool, BinaryOperationError> {
    match (a, b) {
        (ReadyValue::String(needle), ReadyValue::String(haystack)) => Ok(haystack.contains(needle)),
        (value, ReadyValue::List(items)) => Ok(items
            .iter()
            .any(|item| matches!(item, Value::Ready(item) if item == value))),
        (ReadyValue::String(needle), ReadyValue::Dict(entries)) => Ok(entries.contains_key(needle)),
        (_, ReadyValue::Dict(_)) => Ok(false),
        _ => Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::In,
        }),
    }
}

fn non_nan_finite_from<T>(
    value: T,
    operation: BinaryOperationKind,
) -> Result<NonNaNFinite, BinaryOperationError>
where
    T: TryInto<NonNaNFinite>,
{
    value
        .try_into()
        .map_err(|_| BinaryOperationError::ResultOutOfBounds { operation })
}

fn normalized_index(index: &ReadyValue, len: usize) -> Result<usize, IndexOperationError> {
    match index {
        ReadyValue::Int(index) => {
            pythonic::normalized_index(*index, len).ok_or(IndexOperationError::IndexOutOfBounds)
        }
        ReadyValue::Float(_)
        | ReadyValue::Bool(_)
        | ReadyValue::String(_)
        | ReadyValue::None
        | ReadyValue::List(_)
        | RV::Dict(_)
        | RV::Exception(_) => Err(IndexOperationError::UnsupportedOperation),
    }
}

impl<ConstValue> waymark_vm_interpreter_pureset::operations::LoadConst<ReadyValue, ConstValue>
    for PythonVariation
where
    ReadyValue: From<ConstValue>,
{
    fn load_const(const_value: ConstValue) -> ReadyValue {
        const_value.into()
    }
}

impl waymark_vm_interpreter_pureset::operations::BinaryOps<ReadyValue> for PythonVariation {
    type Error = BinaryOperationError;

    fn add(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => left
                .checked_add(*right)
                .map(ReadyValue::Int)
                .ok_or(BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Add,
                }),
            (ReadyValue::String(left), ReadyValue::String(right)) => {
                let mut value = left.clone();
                value.push_str(right);
                Ok(ReadyValue::String(value))
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() + right.get(), BinaryOperationKind::Add)?;
                Ok(ReadyValue::Float(value))
            }
            (ReadyValue::List(left), ReadyValue::List(right)) => {
                let mut items = left.clone();
                items.extend(right.clone());
                Ok(ReadyValue::List(items))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Add,
            }),
        }
    }

    fn sub(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => left
                .checked_sub(*right)
                .map(ReadyValue::Int)
                .ok_or(BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Sub,
                }),
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() - right.get(), BinaryOperationKind::Sub)?;
                Ok(ReadyValue::Float(value))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Sub,
            }),
        }
    }

    fn mul(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => left
                .checked_mul(*right)
                .map(ReadyValue::Int)
                .ok_or(BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Mul,
                }),
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() * right.get(), BinaryOperationKind::Mul)?;
                Ok(ReadyValue::Float(value))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Mul,
            }),
        }
    }

    fn div(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(_), ReadyValue::Int(0)) => Err(BinaryOperationError::DivisionByZero {
                operation: BinaryOperationKind::Div,
            }),
            (ReadyValue::Int(left), ReadyValue::Int(right)) => {
                let quotient =
                    left.checked_div(*right)
                        .ok_or(BinaryOperationError::ResultOutOfBounds {
                            operation: BinaryOperationKind::Div,
                        })?;
                let remainder =
                    left.checked_rem(*right)
                        .ok_or(BinaryOperationError::ResultOutOfBounds {
                            operation: BinaryOperationKind::Div,
                        })?;

                if remainder == 0 {
                    Ok(ReadyValue::Int(quotient))
                } else {
                    Err(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::Div,
                    })
                }
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() / right.get(), BinaryOperationKind::Div)?;
                Ok(ReadyValue::Float(value))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Div,
            }),
        }
    }

    fn floor_div(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(_), ReadyValue::Int(0)) => Err(BinaryOperationError::DivisionByZero {
                operation: BinaryOperationKind::FloorDiv,
            }),
            (ReadyValue::Int(left), ReadyValue::Int(right)) => {
                pythonic::checked_floor_div_i64(*left, *right)
                    .map(ReadyValue::Int)
                    .ok_or(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::FloorDiv,
                    })
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                pythonic::checked_floor_div_float(*left, *right)
                    .map(ReadyValue::Float)
                    .ok_or(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::FloorDiv,
                    })
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::FloorDiv,
            }),
        }
    }

    fn modulo(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(_), ReadyValue::Int(0)) => Err(BinaryOperationError::DivisionByZero {
                operation: BinaryOperationKind::Mod,
            }),
            (ReadyValue::Int(left), ReadyValue::Int(right)) => {
                pythonic::checked_modulo_i64(*left, *right)
                    .map(ReadyValue::Int)
                    .ok_or(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::Mod,
                    })
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                pythonic::checked_modulo_float(*left, *right)
                    .map(ReadyValue::Float)
                    .ok_or(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::Mod,
                    })
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Mod,
            }),
        }
    }

    fn eq(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        Ok(ReadyValue::Bool(a == b))
    }

    fn ne(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        Ok(ReadyValue::Bool(a != b))
    }

    fn lt(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => Ok(ReadyValue::Bool(left < right)),
            (ReadyValue::String(left), ReadyValue::String(right)) => {
                Ok(ReadyValue::Bool(left < right))
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                Ok(ReadyValue::Bool(left < right))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Lt,
            }),
        }
    }

    fn le(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => Ok(ReadyValue::Bool(left <= right)),
            (ReadyValue::String(left), ReadyValue::String(right)) => {
                Ok(ReadyValue::Bool(left <= right))
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                Ok(ReadyValue::Bool(left <= right))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Le,
            }),
        }
    }

    fn gt(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => Ok(ReadyValue::Bool(left > right)),
            (ReadyValue::String(left), ReadyValue::String(right)) => {
                Ok(ReadyValue::Bool(left > right))
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                Ok(ReadyValue::Bool(left > right))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Gt,
            }),
        }
    }

    fn ge(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match (a, b) {
            (ReadyValue::Int(left), ReadyValue::Int(right)) => Ok(ReadyValue::Bool(left >= right)),
            (ReadyValue::String(left), ReadyValue::String(right)) => {
                Ok(ReadyValue::Bool(left >= right))
            }
            (ReadyValue::Float(left), ReadyValue::Float(right)) => {
                Ok(ReadyValue::Bool(left >= right))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Ge,
            }),
        }
    }

    fn contains(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        contains_bool(a, b).map(ReadyValue::Bool)
    }

    fn not_contains(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        contains_bool(a, b).map(|value| ReadyValue::Bool(!value))
    }

    fn and(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        if a.is_truthy() {
            Ok(b.clone())
        } else {
            Ok(a.clone())
        }
    }

    fn or(a: &ReadyValue, b: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        if a.is_truthy() {
            Ok(a.clone())
        } else {
            Ok(b.clone())
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::UnaryOps<ReadyValue> for PythonVariation {
    type Error = UnaryOperationError;

    fn neg(value: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        match value {
            ReadyValue::Int(value) => value.checked_neg().map(ReadyValue::Int).ok_or(
                UnaryOperationError::ResultOutOfBounds {
                    operation: UnaryOperationKind::Neg,
                },
            ),
            ReadyValue::Float(value) => Ok(ReadyValue::Float(-*value)),
            _ => Err(UnaryOperationError::UnsupportedOperation {
                operation: UnaryOperationKind::Neg,
            }),
        }
    }

    fn not(value: &ReadyValue) -> Result<ReadyValue, Self::Error> {
        Ok(ReadyValue::Bool(!value.is_truthy()))
    }
}

impl waymark_vm_interpreter_pureset::operations::AsDictKey<ReadyValue> for PythonVariation {
    type Error = AsDictKeyError;

    fn as_dict_key(value: &ReadyValue) -> Result<&str, Self::Error> {
        match value {
            ReadyValue::String(value) => Ok(value),
            ReadyValue::Int(_)
            | ReadyValue::Float(_)
            | ReadyValue::Bool(_)
            | ReadyValue::None
            | ReadyValue::List(_)
            | ReadyValue::Dict(_)
            | ReadyValue::Exception(_) => Err(AsDictKeyError::UnsupportedKeyType),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::Length<ReadyValue> for PythonVariation {
    type Error = LengthError;
    type FromLengthError = FromLengthError;

    type Length = usize;

    fn length(value: &ReadyValue) -> Result<Self::Length, Self::Error> {
        match value {
            ReadyValue::String(value) => Ok(value.len()),
            ReadyValue::List(items) => Ok(items.len()),
            ReadyValue::Dict(entries) => Ok(entries.len()),
            ReadyValue::Int(_)
            | ReadyValue::Float(_)
            | ReadyValue::Bool(_)
            | ReadyValue::None
            | ReadyValue::Exception(_) => Err(LengthError::UnsupportedValue),
        }
    }

    fn from_length(length: Self::Length) -> Result<ReadyValue, Self::FromLengthError> {
        let value = i64::try_from(length).map_err(|_| FromLengthError::ResultOutOfBounds)?;
        Ok(ReadyValue::Int(value))
    }
}

impl waymark_vm_interpreter_pureset::operations::IndexOp<ReadyValue> for PythonVariation {
    type Error = IndexOperationError;

    fn index(object: &ReadyValue, index: &ReadyValue) -> Result<Value, Self::Error> {
        match object {
            ReadyValue::List(items) => {
                let index = normalized_index(index, items.len())?;
                Ok(items[index].clone())
            }
            ReadyValue::String(value) => {
                let index = normalized_index(index, value.chars().count())?;
                let character = value
                    .chars()
                    .nth(index)
                    .expect("normalized string index should be in bounds");
                Ok(Value::Ready(ReadyValue::String(character.to_string())))
            }
            ReadyValue::Dict(entries) => match index {
                ReadyValue::String(key) => entries
                    .get(key)
                    .cloned()
                    .ok_or(IndexOperationError::MissingKey),
                ReadyValue::Int(_)
                | ReadyValue::Float(_)
                | ReadyValue::Bool(_)
                | ReadyValue::None
                | ReadyValue::List(_)
                | ReadyValue::Dict(_)
                | ReadyValue::Exception(_) => Err(IndexOperationError::UnsupportedOperation),
            },
            ReadyValue::Int(_)
            | ReadyValue::Float(_)
            | ReadyValue::Bool(_)
            | ReadyValue::None
            | ReadyValue::Exception(_) => Err(IndexOperationError::UnsupportedOperation),
        }
    }
}

impl waymark_vm_interpreter_pureset::operations::DotOp<ReadyValue> for PythonVariation {
    type Error = DotOperationError;

    fn dot(object: &ReadyValue, attribute: &str) -> Result<Value, Self::Error> {
        match object {
            ReadyValue::Dict(entries) => entries
                .get(attribute)
                .cloned()
                .ok_or(DotOperationError::MissingAttribute),
            ReadyValue::Int(_)
            | ReadyValue::Float(_)
            | ReadyValue::Bool(_)
            | ReadyValue::String(_)
            | ReadyValue::None
            | ReadyValue::List(_)
            | ReadyValue::Exception(_) => Err(DotOperationError::UnsupportedOperation),
        }
    }
}
