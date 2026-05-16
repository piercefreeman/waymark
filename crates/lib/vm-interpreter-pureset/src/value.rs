//! Value requirements.

use waymark_vm_instructions_pureset::{BinaryOpKind, UnaryOpKind};

/// An error from a binary scalar operation.
#[derive(Debug, thiserror::Error)]
pub enum BinaryOperationError {
    /// The current value type does not support this operation for the operands.
    #[error("{operation} is not supported for these operands")]
    UnsupportedOperation {
        /// The unsupported operation.
        operation: BinaryOpKind,
    },

    /// The result could not be represented by the value type.
    #[error("{operation} result is out of bounds")]
    ResultOutOfBounds {
        /// The operation that overflowed or otherwise could not be represented.
        operation: BinaryOpKind,
    },

    /// The operation attempted to divide by zero.
    #[error("{operation} cannot divide by zero")]
    DivisionByZero {
        /// The operation that attempted division by zero.
        operation: BinaryOpKind,
    },
}

/// An error from a unary scalar operation.
#[derive(Debug, thiserror::Error)]
pub enum UnaryOperationError {
    /// The current value type does not support this operation for the operand.
    #[error("{operation} is not supported for this operand")]
    UnsupportedOperation {
        /// The unsupported operation.
        operation: UnaryOpKind,
    },

    /// The result could not be represented by the value type.
    #[error("{operation} result is out of bounds")]
    ResultOutOfBounds {
        /// The operation that overflowed or otherwise could not be represented.
        operation: UnaryOpKind,
    },
}

/// An error from [`MakeList::make_list`].
#[derive(Debug, thiserror::Error)]
pub enum MakeListError {
    /// The value type does not support list construction.
    #[error("constructing list values is not supported")]
    NotListable,

    /// The resulting list could not be represented by the value type.
    #[error("list result is out of bounds")]
    ResultOutOfBounds,
}

/// An error from [`MakeDict::make_dict`].
#[derive(Debug, thiserror::Error)]
pub enum MakeDictError {
    /// The value type does not support dictionary construction.
    #[error("constructing dict values is not supported")]
    NotDictable,

    /// The value type does not support the provided key type.
    #[error("dict keys of this type are not supported")]
    UnsupportedKeyType,

    /// The resulting dictionary could not be represented by the value type.
    #[error("dict result is out of bounds")]
    ResultOutOfBounds,
}

/// An error from [`Length::length`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LengthError {
    /// The value type does not support reporting a length for this value.
    #[error("determining length is not supported for this value")]
    UnsupportedValue,
}

/// An error from [`Length::from_length`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FromLengthError {
    /// The resulting length could not be represented by the value type.
    #[error("length result is out of bounds")]
    ResultOutOfBounds,
}

/// Apply binary scalar operations to resolved values.
pub trait BinaryOps: Sized {
    /// Add two resolved values together.
    fn add(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Add,
        })
    }

    /// Subtract the right value from the left value.
    fn sub(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Sub,
        })
    }

    /// Multiply two resolved values together.
    fn mul(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Mul,
        })
    }

    /// Divide the left value by the right value.
    fn div(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Div,
        })
    }

    /// Floor-divide the left value by the right value.
    fn floor_div(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::FloorDiv,
        })
    }

    /// Compute the left value modulo the right value.
    fn modulo(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Mod,
        })
    }

    /// Compare two resolved values for equality.
    fn eq(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Eq,
        })
    }

    /// Compare two resolved values for inequality.
    fn ne(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Ne,
        })
    }

    /// Compare whether the left value is less than the right value.
    fn lt(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Lt,
        })
    }

    /// Compare whether the left value is less than or equal to the right value.
    fn le(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Le,
        })
    }

    /// Compare whether the left value is greater than the right value.
    fn gt(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Gt,
        })
    }

    /// Compare whether the left value is greater than or equal to the right value.
    fn ge(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Ge,
        })
    }

    /// Test whether the left value is contained in the right value.
    fn contains(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::In,
        })
    }

    /// Test whether the left value is not contained in the right value.
    fn not_contains(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::NotIn,
        })
    }

    /// Apply Python-style logical `and` to two resolved values.
    fn and(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::And,
        })
    }

    /// Apply Python-style logical `or` to two resolved values.
    fn or(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Or,
        })
    }
}

/// Apply unary scalar operations to resolved values.
pub trait UnaryOps: Sized {
    /// Negate the resolved operand value.
    fn neg(value: &Self) -> Result<Self, UnaryOperationError> {
        let _ = value;
        Err(UnaryOperationError::UnsupportedOperation {
            operation: UnaryOpKind::Neg,
        })
    }

    /// Apply Python-style logical `not` to the resolved operand value.
    fn not(value: &Self) -> Result<Self, UnaryOperationError> {
        let _ = value;
        Err(UnaryOperationError::UnsupportedOperation {
            operation: UnaryOpKind::Not,
        })
    }
}

/// Build a list value from a sequence of resolved items.
pub trait MakeList: Sized {
    /// Construct a list value preserving input order.
    fn make_list<I>(items: I) -> Result<Self, MakeListError>
    where
        I: IntoIterator<Item = Self>;
}

/// Build a dictionary value from a sequence of resolved key-value pairs.
pub trait MakeDict: Sized {
    /// Construct a dictionary value preserving entry order.
    fn make_dict<I>(entries: I) -> Result<Self, MakeDictError>
    where
        I: IntoIterator<Item = (Self, Self)>;
}

/// Compute and materialize container lengths.
pub trait Length: Sized {
    /// The type for internal representation of a value length.
    type Length;

    /// Determine the length of the resolved value.
    fn length(&self) -> Result<Self::Length, LengthError>;

    /// Materialize a length result back into the VM value type.
    fn from_length(length: Self::Length) -> Result<Self, FromLengthError>;
}

/// A unifying trait for all value requirements.
pub trait Value: BinaryOps + UnaryOps + MakeList + MakeDict + Length {}

impl<T> Value for T where T: BinaryOps + UnaryOps + MakeList + MakeDict + Length {}
