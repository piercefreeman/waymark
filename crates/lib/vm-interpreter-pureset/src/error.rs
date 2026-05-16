use waymark_vm_runtime_core::{RegisterId, UnresolvedPromiseError};

use waymark_vm_instructions_pureset::{BinaryOpKind, UnaryOpKind};

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
    /// A `Copy` instruction referenced an unset register.
    #[error("copy source in register {register:?} is not initialized")]
    MissingCopySource {
        /// The register that was read.
        register: RegisterId,
    },

    /// A binary scalar instruction referenced an unset register.
    #[error("{operand_pos} {operation} operand in register {register:?} is not initialized")]
    MissingBinaryOperand {
        /// The binary operation being evaluated.
        operation: BinaryOpKind,

        /// The operand position.
        operand_pos: BinaryOperandPosition,

        /// The register that was read.
        register: RegisterId,
    },

    /// A binary scalar instruction referenced an unresolved promise.
    #[error("{operand_pos} {operation} operand is unresolved: {source}")]
    UnresolvedBinaryOperand {
        /// The binary operation being evaluated.
        operation: BinaryOpKind,

        /// The operand position.
        operand_pos: BinaryOperandPosition,

        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// A unary scalar instruction referenced an unset register.
    #[error("{operation} operand in register {register:?} is not initialized")]
    MissingUnaryOperand {
        /// The unary operation being evaluated.
        operation: UnaryOpKind,

        /// The register that was read.
        register: RegisterId,
    },

    /// A unary scalar instruction referenced an unresolved promise.
    #[error("{operation} operand is unresolved: {source}")]
    UnresolvedUnaryOperand {
        /// The unary operation being evaluated.
        operation: UnaryOpKind,

        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// A `Length` instruction referenced an unset register.
    #[error("length operand in register {register:?} is not initialized")]
    MissingLengthOperand {
        /// The register that was read.
        register: RegisterId,
    },

    /// A `Length` instruction referenced an unresolved promise.
    #[error("length operand is unresolved: {source}")]
    UnresolvedLengthOperand {
        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// A `MakeList` instruction referenced an unset register.
    #[error("list item {item_pos} in register {register:?} is not initialized")]
    MissingListItem {
        /// The zero-based item position.
        item_pos: usize,

        /// The register that was read.
        register: RegisterId,
    },

    /// A `MakeList` instruction referenced an unresolved promise.
    #[error("list item {item_pos} is unresolved: {source}")]
    UnresolvedListItem {
        /// The zero-based item position.
        item_pos: usize,

        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// A `MakeDict` instruction referenced an unset key register.
    #[error("dict entry {entry_pos} key in register {register:?} is not initialized")]
    MissingDictKey {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The register that was read.
        register: RegisterId,
    },

    /// A `MakeDict` instruction referenced an unresolved key promise.
    #[error("dict entry {entry_pos} key is unresolved: {source}")]
    UnresolvedDictKey {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// A `MakeDict` instruction referenced an unset value register.
    #[error("dict entry {entry_pos} value in register {register:?} is not initialized")]
    MissingDictValue {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The register that was read.
        register: RegisterId,
    },

    /// A `MakeDict` instruction referenced an unresolved value promise.
    #[error("dict entry {entry_pos} value is unresolved: {source}")]
    UnresolvedDictValue {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The underlying unresolved promise error.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// Evaluating a binary scalar instruction failed.
    #[error("{operation}: {source}")]
    BinaryOperation {
        /// The binary operation that failed.
        operation: BinaryOpKind,

        /// The operation-specific failure.
        #[source]
        source: crate::value::BinaryOperationError,
    },

    /// Evaluating a unary scalar instruction failed.
    #[error("{operation}: {source}")]
    UnaryOperation {
        /// The unary operation that failed.
        operation: UnaryOpKind,

        /// The operation-specific failure.
        #[source]
        source: crate::value::UnaryOperationError,
    },

    /// Evaluating a `Length` instruction failed.
    #[error("length: {0}")]
    Length(#[source] crate::value::LengthError),

    /// Materializing the result of a `Length` instruction failed.
    #[error("length result: {0}")]
    FromLength(#[source] crate::value::FromLengthError),

    /// Evaluating a `MakeList` instruction failed.
    #[error("make_list: {0}")]
    MakeList(#[source] crate::value::MakeListError),

    /// Evaluating a `MakeDict` instruction failed.
    #[error("make_dict: {0}")]
    MakeDict(#[source] crate::value::MakeDictError),
}
