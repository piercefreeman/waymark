//! [`waymark_vm_interpreter_pureset`] trait implementations for [`Value`].

use typed_floats::NonNaNFinite;
use waymark_vm_instructions_pureset::{
    BinaryOpKind as BinaryOperationKind, UnaryOpKind as UnaryOperationKind,
};
use waymark_vm_interpreter_pureset::value::{BinaryOperationError, UnaryOperationError};

use crate::{Value, pythonic};

fn contains_bool(a: &Value, b: &Value) -> Result<bool, BinaryOperationError> {
    match (a, b) {
        (Value::String(needle), Value::String(haystack)) => Ok(haystack.contains(needle)),
        (value, Value::List(items)) => Ok(items.iter().any(|item| item == value)),
        (Value::String(needle), Value::Dict(entries)) => Ok(entries.contains_key(needle)),
        (_, Value::Dict(_)) => Ok(false),
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

impl waymark_vm_interpreter_pureset::value::BinaryOps for Value {
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

impl waymark_vm_interpreter_pureset::value::UnaryOps for Value {
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

impl waymark_vm_interpreter_pureset::value::MakeList for Value {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self>,
    {
        Ok(Self::List(items.into_iter().collect()))
    }
}
