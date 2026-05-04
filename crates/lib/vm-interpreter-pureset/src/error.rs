use waymark_vm_runtime_core::{RegisterId, UnresolvedPromiseError};

/// A specified of the operand position in a binary operation.
///
/// Used in the errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BinaryOperandPosition {
    /// First operand.
    First,

    /// Second operand.
    Second,
}

impl core::fmt::Display for BinaryOperandPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperandPosition::First => write!(f, "first"),
            BinaryOperandPosition::Second => write!(f, "second"),
        }
    }
}

/// The error for the [`crate::PureSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An `Add` instruction referenced an unset register.
    #[error(" {operand_pos} add operand in register {register:?} is not initialized")]
    MissingAddOperand {
        /// The operand position.
        operand_pos: BinaryOperandPosition,

        /// The register that was read.
        register: RegisterId,
    },

    /// An `Add` instruction referenced an unresolved promise.
    #[error("{operand_pos} add operand is unresolved: {source}")]
    UnresolvedAddOperand {
        /// The operand position.
        operand_pos: BinaryOperandPosition,

        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// Evaluating an `Add` instruction failed.
    #[error("add: {0}")]
    Add(#[source] crate::value::AddError),
}
