//! [`waymark_vm_interpreter_pureset`] trait implementations for [`Value`].

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::{
    BinaryOpKind as BinaryOperationKind, UnaryOpKind as UnaryOperationKind,
};
use waymark_vm_interpreter_pureset::value::{
    AsDictKeyError, BinaryOperationError, DotOperationError, FromLengthError, IndexOperationError,
    LengthError, MakeDictError, UnaryOperationError,
};

use crate::{ReadyValue, ReadyValue as RV, Value, pythonic};

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
        | RV::Dict(_) => Err(IndexOperationError::UnsupportedOperation),
    }
}

impl<ConstValue> waymark_vm_interpreter_pureset::value::LoadConst<ConstValue> for ReadyValue
where
    Self: From<ConstValue>,
{
    fn load_const(const_value: ConstValue) -> Self {
        const_value.into()
    }
}

impl waymark_vm_interpreter_pureset::value::CaptureCopy for ReadyValue {
    fn capture_copy(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_pureset::value::BinaryOps for ReadyValue {
    fn add(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => left.checked_add(*right).map(Self::Int).ok_or(
                BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Add,
                },
            ),
            (Self::String(left), Self::String(right)) => {
                let mut value = left.clone();
                value.push_str(right);
                Ok(Self::String(value))
            }
            (Self::Float(left), Self::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() + right.get(), BinaryOperationKind::Add)?;
                Ok(Self::Float(value))
            }
            (Self::List(left), Self::List(right)) => {
                let mut items = left.clone();
                items.extend(right.clone());
                Ok(Self::List(items))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Add,
            }),
        }
    }

    fn sub(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => left.checked_sub(*right).map(Self::Int).ok_or(
                BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Sub,
                },
            ),
            (Self::Float(left), Self::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() - right.get(), BinaryOperationKind::Sub)?;
                Ok(Self::Float(value))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Sub,
            }),
        }
    }

    fn mul(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => left.checked_mul(*right).map(Self::Int).ok_or(
                BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Mul,
                },
            ),
            (Self::Float(left), Self::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() * right.get(), BinaryOperationKind::Mul)?;
                Ok(Self::Float(value))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Mul,
            }),
        }
    }

    fn div(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(_), Self::Int(0)) => Err(BinaryOperationError::DivisionByZero {
                operation: BinaryOperationKind::Div,
            }),
            (Self::Int(left), Self::Int(right)) => {
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
                    Ok(Self::Int(quotient))
                } else {
                    Err(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::Div,
                    })
                }
            }
            (Self::Float(left), Self::Float(right)) => {
                let value =
                    non_nan_finite_from(left.get() / right.get(), BinaryOperationKind::Div)?;
                Ok(Self::Float(value))
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Div,
            }),
        }
    }

    fn floor_div(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(_), Self::Int(0)) => Err(BinaryOperationError::DivisionByZero {
                operation: BinaryOperationKind::FloorDiv,
            }),
            (Self::Int(left), Self::Int(right)) => pythonic::checked_floor_div_i64(*left, *right)
                .map(Self::Int)
                .ok_or(BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::FloorDiv,
                }),
            (Self::Float(left), Self::Float(right)) => {
                pythonic::checked_floor_div_float(*left, *right)
                    .map(Self::Float)
                    .ok_or(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::FloorDiv,
                    })
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::FloorDiv,
            }),
        }
    }

    fn modulo(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(_), Self::Int(0)) => Err(BinaryOperationError::DivisionByZero {
                operation: BinaryOperationKind::Mod,
            }),
            (Self::Int(left), Self::Int(right)) => pythonic::checked_modulo_i64(*left, *right)
                .map(Self::Int)
                .ok_or(BinaryOperationError::ResultOutOfBounds {
                    operation: BinaryOperationKind::Mod,
                }),
            (Self::Float(left), Self::Float(right)) => {
                pythonic::checked_modulo_float(*left, *right)
                    .map(Self::Float)
                    .ok_or(BinaryOperationError::ResultOutOfBounds {
                        operation: BinaryOperationKind::Mod,
                    })
            }
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Mod,
            }),
        }
    }

    fn eq(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        Ok(Self::Bool(a == b))
    }

    fn ne(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        Ok(Self::Bool(a != b))
    }

    fn lt(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => Ok(Self::Bool(left < right)),
            (Self::String(left), Self::String(right)) => Ok(Self::Bool(left < right)),
            (Self::Float(left), Self::Float(right)) => Ok(Self::Bool(left < right)),
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Lt,
            }),
        }
    }

    fn le(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => Ok(Self::Bool(left <= right)),
            (Self::String(left), Self::String(right)) => Ok(Self::Bool(left <= right)),
            (Self::Float(left), Self::Float(right)) => Ok(Self::Bool(left <= right)),
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Le,
            }),
        }
    }

    fn gt(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => Ok(Self::Bool(left > right)),
            (Self::String(left), Self::String(right)) => Ok(Self::Bool(left > right)),
            (Self::Float(left), Self::Float(right)) => Ok(Self::Bool(left > right)),
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Gt,
            }),
        }
    }

    fn ge(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        match (a, b) {
            (Self::Int(left), Self::Int(right)) => Ok(Self::Bool(left >= right)),
            (Self::String(left), Self::String(right)) => Ok(Self::Bool(left >= right)),
            (Self::Float(left), Self::Float(right)) => Ok(Self::Bool(left >= right)),
            _ => Err(BinaryOperationError::UnsupportedOperation {
                operation: BinaryOperationKind::Ge,
            }),
        }
    }

    fn contains(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        contains_bool(a, b).map(Self::Bool)
    }

    fn not_contains(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        contains_bool(a, b).map(|value| Self::Bool(!value))
    }

    fn and(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        if a.is_truthy() {
            Ok(b.clone())
        } else {
            Ok(a.clone())
        }
    }

    fn or(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        if a.is_truthy() {
            Ok(a.clone())
        } else {
            Ok(b.clone())
        }
    }
}

impl waymark_vm_interpreter_pureset::value::UnaryOps for ReadyValue {
    fn neg(value: &Self) -> Result<Self, UnaryOperationError> {
        match value {
            Self::Int(value) => {
                value
                    .checked_neg()
                    .map(Self::Int)
                    .ok_or(UnaryOperationError::ResultOutOfBounds {
                        operation: UnaryOperationKind::Neg,
                    })
            }
            Self::Float(value) => Ok(Self::Float(-*value)),
            _ => Err(UnaryOperationError::UnsupportedOperation {
                operation: UnaryOperationKind::Neg,
            }),
        }
    }

    fn not(value: &Self) -> Result<Self, UnaryOperationError> {
        Ok(Self::Bool(!value.is_truthy()))
    }
}

impl waymark_vm_interpreter_pureset::value::MakeList for ReadyValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self::RootValue>,
    {
        Ok(Self::List(items.into_iter().collect()))
    }
}

impl waymark_vm_interpreter_pureset::value::ListAppend for ReadyValue {
    fn list_append(
        list: &Self,
        item: Self::RootValue,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::ListAppendError> {
        let Self::List(existing) = list else {
            return Err(waymark_vm_interpreter_pureset::value::ListAppendError::NotListable);
        };
        let mut grown = Vec::with_capacity(existing.len() + 1);
        grown.extend(existing.iter().cloned());
        grown.push(item);
        Ok(Self::List(grown))
    }
}

impl waymark_vm_interpreter_pureset::value::AsDictKey for ReadyValue {
    fn as_dict_key(&self) -> Result<&str, AsDictKeyError> {
        match self {
            Self::String(value) => Ok(value),
            Self::Int(_)
            | Self::Float(_)
            | Self::Bool(_)
            | Self::None
            | Self::List(_)
            | Self::Dict(_) => Err(AsDictKeyError::UnsupportedKeyType),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::MakeDict for ReadyValue {
    fn make_dict<I>(entries: I) -> Result<Self, MakeDictError>
    where
        I: IntoIterator<Item = (String, Self::RootValue)>,
    {
        let mut dict = IndexMap::new();

        for (key, value) in entries {
            dict.insert(key, value);
        }

        Ok(Self::Dict(dict))
    }
}

impl waymark_vm_interpreter_pureset::value::Length for ReadyValue {
    type Length = usize;

    fn length(&self) -> Result<Self::Length, LengthError> {
        match self {
            Self::String(value) => Ok(value.len()),
            Self::List(items) => Ok(items.len()),
            Self::Dict(entries) => Ok(entries.len()),
            Self::Int(_) | Self::Float(_) | Self::Bool(_) | Self::None => {
                Err(LengthError::UnsupportedValue)
            }
        }
    }

    fn from_length(length: Self::Length) -> Result<Self, FromLengthError> {
        let value = i64::try_from(length).map_err(|_| FromLengthError::ResultOutOfBounds)?;
        Ok(Self::Int(value))
    }
}

impl waymark_vm_interpreter_pureset::value::IndexOp for ReadyValue {
    fn index(object: &Self, index: &Self) -> Result<Self::RootValue, IndexOperationError> {
        match object {
            Self::List(items) => {
                let index = normalized_index(index, items.len())?;
                Ok(items[index].clone())
            }
            Self::String(value) => {
                let index = normalized_index(index, value.chars().count())?;
                let character = value
                    .chars()
                    .nth(index)
                    .expect("normalized string index should be in bounds");
                Ok(Value::Ready(Self::String(character.to_string())))
            }
            Self::Dict(entries) => match index {
                Self::String(key) => entries
                    .get(key)
                    .cloned()
                    .ok_or(IndexOperationError::MissingKey),
                Self::Int(_)
                | Self::Float(_)
                | Self::Bool(_)
                | Self::None
                | Self::List(_)
                | Self::Dict(_) => Err(IndexOperationError::UnsupportedOperation),
            },
            Self::Int(_) | Self::Float(_) | Self::Bool(_) | Self::None => {
                Err(IndexOperationError::UnsupportedOperation)
            }
        }
    }
}

impl waymark_vm_interpreter_pureset::value::DotOp for ReadyValue {
    fn dot(object: &Self, attribute: &str) -> Result<Self::RootValue, DotOperationError> {
        match object {
            Self::Dict(entries) => entries
                .get(attribute)
                .cloned()
                .ok_or(DotOperationError::MissingAttribute),
            Self::Int(_)
            | Self::Float(_)
            | Self::Bool(_)
            | Self::String(_)
            | Self::None
            | Self::List(_) => Err(DotOperationError::UnsupportedOperation),
        }
    }
}
