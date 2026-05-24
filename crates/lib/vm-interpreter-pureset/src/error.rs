use waymark_vm_runtime_core::RegisterId;

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

    /// A binary instruction referenced an unset register.
    #[error("{operand_pos} {operation} operand in register {register:?} is not initialized")]
    MissingBinaryOperand {
        /// The binary operation being evaluated.
        operation: BinaryOpKind,

        /// The operand position.
        operand_pos: BinaryOperandPosition,

        /// The register that was read.
        register: RegisterId,
    },

    /// A binary instruction referenced an unusable value.
    #[error("{operand_pos} {operation} operand is unusable: {source}")]
    UnusableBinaryOperand {
        /// The binary operation being evaluated.
        operation: BinaryOpKind,

        /// The operand position.
        operand_pos: BinaryOperandPosition,

        /// The underlying error.
        #[source]
        source: crate::AsScalarError,
    },

    /// A unary instruction referenced an unset register.
    #[error("{operation} operand in register {register:?} is not initialized")]
    MissingUnaryOperand {
        /// The unary operation being evaluated.
        operation: UnaryOpKind,

        /// The register that was read.
        register: RegisterId,
    },

    /// A unary instruction referenced an unusable value.
    #[error("{operation} operand is unusable: {source}")]
    UnusableUnaryOperand {
        /// The unary operation being evaluated.
        operation: UnaryOpKind,

        /// The underlying error.
        #[source]
        source: crate::value::AsScalarError,
    },

    /// A `Length` instruction referenced an unset register.
    #[error("length value in register {register:?} is not initialized")]
    MissingLengthValue {
        /// The register that was read.
        register: RegisterId,
    },

    /// A `Length` instruction referenced an unrepresentable promise.
    #[error("length value is unusable: {source}")]
    UnusableLengthValue {
        /// The underlying error.
        #[source]
        source: crate::LengthError,
    },

    /// A `MakeList` instruction referenced an unset register.
    #[error("list item {item_pos} in register {register:?} is not initialized")]
    MissingListItem {
        /// The zero-based item position.
        item_pos: usize,

        /// The register that was read.
        register: RegisterId,
    },

    /// A `ListAppend` instruction referenced an unset list register.
    #[error("list append source in register {register:?} is not initialized")]
    MissingListAppendList {
        /// The register that was read.
        register: RegisterId,
    },

    /// A `ListAppend` instruction referenced an unset item register.
    #[error("list append item in register {register:?} is not initialized")]
    MissingListAppendItem {
        /// The register that was read.
        register: RegisterId,
    },

    /// A `MakeDict` instruction referenced an unset key register.
    #[error("dict entry {entry_pos} key in register {register:?} is not initialized")]
    MissingDictKey {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The register that was read.
        register: RegisterId,
    },

    /// A `MakeDict` instruction referenced an unrepresentable key promise.
    #[error("dict entry {entry_pos} key is unusable: {source}")]
    UnusableDictKey {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The underlying representation error.
        #[source]
        source: crate::value::AsDictKeyError,
    },

    /// A `MakeDict` instruction referenced an unset value register.
    #[error("dict entry {entry_pos} value in register {register:?} is not initialized")]
    MissingDictValue {
        /// The zero-based dictionary entry position.
        entry_pos: usize,

        /// The register that was read.
        register: RegisterId,
    },

    /// An `Index` instruction referenced an unset object register.
    #[error("index object in register {register:?} is not initialized")]
    MissingIndexObject {
        /// The register that was read.
        register: RegisterId,
    },

    /// An `Index` instruction referenced an unset index register.
    #[error("index operand in register {register:?} is not initialized")]
    MissingIndexOperand {
        /// The register that was read.
        register: RegisterId,
    },

    /// A `Dot` instruction referenced an unset object register.
    #[error("dot object for attribute {attribute:?} in register {register:?} is not initialized")]
    MissingDotObject {
        /// The accessed attribute name.
        attribute: String,

        /// The register that was read.
        register: RegisterId,
    },

    /// Evaluating a binary instruction failed.
    #[error("{operation}: {source}")]
    BinaryOperation {
        /// The binary operation that failed.
        operation: BinaryOpKind,

        /// The operation-specific failure.
        #[source]
        source: crate::value::BinaryOperationError,
    },

    /// Evaluating a unary instruction failed.
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

    /// Evaluating a `ListAppend` instruction failed.
    #[error("list_append: {0}")]
    ListAppend(#[source] crate::value::ListAppendError),

    /// Evaluating a `MakeDict` instruction failed.
    #[error("make_dict: {0}")]
    MakeDict(#[source] crate::value::MakeDictError),

    /// Evaluating an `Index` instruction failed.
    #[error("index: {source}")]
    IndexOperation {
        /// The operation-specific failure.
        #[source]
        source: crate::value::IndexOperationError,
    },

    /// Evaluating a `Dot` instruction failed.
    #[error("dot attribute {attribute:?}: {source}")]
    DotOperation {
        /// The accessed attribute name.
        attribute: String,

        /// The operation-specific failure.
        #[source]
        source: crate::value::DotOperationError,
    },
}
