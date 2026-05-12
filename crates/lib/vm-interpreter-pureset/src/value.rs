//! Value requirements.

/// A supported binary scalar operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperationKind {
    /// Addition.
    Add,

    /// Subtraction.
    Sub,

    /// Multiplication.
    Mul,

    /// Division.
    Div,

    /// Floor division.
    FloorDiv,

    /// Modulo.
    Mod,

    /// Equality.
    Eq,

    /// Inequality.
    Ne,

    /// Less-than comparison.
    Lt,

    /// Less-than-or-equal comparison.
    Le,

    /// Greater-than comparison.
    Gt,

    /// Greater-than-or-equal comparison.
    Ge,

    /// Membership test.
    In,

    /// Negated membership test.
    NotIn,

    /// Logical `and`.
    And,

    /// Logical `or`.
    Or,
}

impl core::fmt::Display for BinaryOperationKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::FloorDiv => "//",
            Self::Mod => "%",
            Self::Eq => "==",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::In => "in",
            Self::NotIn => "not in",
            Self::And => "and",
            Self::Or => "or",
        })
    }
}

/// An error from a binary scalar operation.
#[derive(Debug, thiserror::Error)]
pub enum BinaryOperationError {
    /// The current value type does not support this operation for the operands.
    #[error("{operation} is not supported for these operands")]
    UnsupportedOperation {
        /// The unsupported operation.
        operation: BinaryOperationKind,
    },

    /// The result could not be represented by the value type.
    #[error("{operation} result is out of bounds")]
    ResultOutOfBounds {
        /// The operation that overflowed or otherwise could not be represented.
        operation: BinaryOperationKind,
    },

    /// The operation attempted to divide by zero.
    #[error("{operation} cannot divide by zero")]
    DivisionByZero {
        /// The operation that attempted division by zero.
        operation: BinaryOperationKind,
    },
}

/// A supported unary scalar operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOperationKind {
    /// Numeric negation.
    Neg,

    /// Logical negation.
    Not,
}

impl core::fmt::Display for UnaryOperationKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::Neg => "-",
            Self::Not => "not",
        })
    }
}

/// An error from a unary scalar operation.
#[derive(Debug, thiserror::Error)]
pub enum UnaryOperationError {
    /// The current value type does not support this operation for the operand.
    #[error("{operation} is not supported for this operand")]
    UnsupportedOperation {
        /// The unsupported operation.
        operation: UnaryOperationKind,
    },

    /// The result could not be represented by the value type.
    #[error("{operation} result is out of bounds")]
    ResultOutOfBounds {
        /// The operation that overflowed or otherwise could not be represented.
        operation: UnaryOperationKind,
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

/// Apply binary scalar operations to resolved values.
pub trait BinaryOps: Sized {
    /// Add two resolved values together.
    fn add(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Add,
        })
    }

    /// Subtract the right value from the left value.
    fn sub(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Sub,
        })
    }

    /// Multiply two resolved values together.
    fn mul(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Mul,
        })
    }

    /// Divide the left value by the right value.
    fn div(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Div,
        })
    }

    /// Floor-divide the left value by the right value.
    fn floor_div(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::FloorDiv,
        })
    }

    /// Compute the left value modulo the right value.
    fn modulo(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Mod,
        })
    }

    /// Compare two resolved values for equality.
    fn eq(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Eq,
        })
    }

    /// Compare two resolved values for inequality.
    fn ne(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Ne,
        })
    }

    /// Compare whether the left value is less than the right value.
    fn lt(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Lt,
        })
    }

    /// Compare whether the left value is less than or equal to the right value.
    fn le(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Le,
        })
    }

    /// Compare whether the left value is greater than the right value.
    fn gt(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Gt,
        })
    }

    /// Compare whether the left value is greater than or equal to the right value.
    fn ge(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Ge,
        })
    }

    /// Test whether the left value is contained in the right value.
    fn contains(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::In,
        })
    }

    /// Test whether the left value is not contained in the right value.
    fn not_contains(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::NotIn,
        })
    }

    /// Apply Python-style logical `and` to two resolved values.
    fn and(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::And,
        })
    }

    /// Apply Python-style logical `or` to two resolved values.
    fn or(a: &Self, b: &Self) -> Result<Self, BinaryOperationError> {
        let _ = (a, b);
        Err(BinaryOperationError::UnsupportedOperation {
            operation: BinaryOperationKind::Or,
        })
    }
}

/// Apply unary scalar operations to resolved values.
pub trait UnaryOps: Sized {
    /// Negate the resolved operand value.
    fn neg(value: &Self) -> Result<Self, UnaryOperationError> {
        let _ = value;
        Err(UnaryOperationError::UnsupportedOperation {
            operation: UnaryOperationKind::Neg,
        })
    }

    /// Apply Python-style logical `not` to the resolved operand value.
    fn not(value: &Self) -> Result<Self, UnaryOperationError> {
        let _ = value;
        Err(UnaryOperationError::UnsupportedOperation {
            operation: UnaryOperationKind::Not,
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

/// A unifying trait for all value requirements.
pub trait Value: BinaryOps + UnaryOps + MakeList {}

impl<T> Value for T where T: BinaryOps + UnaryOps + MakeList {}
