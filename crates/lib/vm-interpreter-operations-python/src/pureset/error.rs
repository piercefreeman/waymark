//! Error types for the Python pureset operations.

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

/// An error from `length`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LengthError {
    /// The value type does not support reporting a length for this value.
    #[error("determining length is not supported for this value")]
    UnsupportedValue,
}

/// An error from `from_length`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FromLengthError {
    /// The resulting length could not be represented by the value type.
    #[error("length result is out of bounds")]
    ResultOutOfBounds,
}

/// An error from `index`.
#[derive(Debug, thiserror::Error)]
pub enum IndexOperationError {
    /// The value type does not support indexed access for the operands.
    #[error("indexed access is not supported for these operands")]
    UnsupportedOperation,

    /// The provided index falls outside the bounds of the target object.
    #[error("index is out of bounds")]
    IndexOutOfBounds,

    /// The target dictionary does not contain the requested key.
    #[error("key is missing")]
    MissingKey,
}

/// An error from `dot`.
#[derive(Debug, thiserror::Error)]
pub enum DotOperationError {
    /// The value type does not support attribute access for this object.
    #[error("attribute access is not supported for this value")]
    UnsupportedOperation,

    /// The target object does not contain the requested attribute.
    #[error("attribute is missing")]
    MissingAttribute,
}

/// An error from `as_dict_key`.
#[derive(Debug, thiserror::Error)]
pub enum AsDictKeyError {
    /// The value can't be used as a dict key.
    #[error("dict keys of this type are not supported")]
    UnsupportedKeyType,
}
