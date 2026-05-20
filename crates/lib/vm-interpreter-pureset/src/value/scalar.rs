use waymark_vm_instructions_pureset::{BinaryOpKind, UnaryOpKind};

/// An error from capturing a value as a scalar.
#[derive(Debug, thiserror::Error)]
pub enum AsScalarError {
    /// The operand was not a scalar.
    #[error("not a scalar")]
    NotAScalar,
}

/// An error from a binary operation.
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

/// An error from a unary operation.
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

/// View a value as a scalar that supports unary and binary operations.
pub trait AsScalar: Sized {
    /// The scalar value type used for arithmetic, comparison, and logical operators.
    type Scalar: BinaryOps + UnaryOps;

    /// Borrow the scalar view of this value.
    fn as_scalar(&self) -> Result<&Self::Scalar, AsScalarError>;

    /// Rewrap a scalar result back into the enclosing value type.
    fn from_scalar(scalar: Self::Scalar) -> Self;
}

/// Apply binary operations to values.
pub trait BinaryOps: Sized {
    /// Add two values together.
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

    /// Multiply two values together.
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

    /// Compare two values for equality.
    fn eq(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Eq,
        })
    }

    /// Compare two values for inequality.
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

    /// Apply Python-style logical `and` to two values.
    fn and(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::And,
        })
    }

    /// Apply Python-style logical `or` to two values.
    fn or(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOpKind::Or,
        })
    }
}

/// Apply unary operations to values.
pub trait UnaryOps: Sized {
    /// Negate the operand value.
    fn neg(value: &Self) -> Result<Self, UnaryOperationError> {
        let _ = value;
        Err(UnaryOperationError::UnsupportedOperation {
            operation: UnaryOpKind::Neg,
        })
    }

    /// Apply Python-style logical `not` to the operand value.
    fn not(value: &Self) -> Result<Self, UnaryOperationError> {
        let _ = value;
        Err(UnaryOperationError::UnsupportedOperation {
            operation: UnaryOpKind::Not,
        })
    }
}
