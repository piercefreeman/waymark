//! The "pure" instruction set for the VM.
//!
//! Responsible for representing all the pure operations and values support.
//! Things like addition, index access.
//!
//! "Pure" here means operation outputs are fully deterministic based on inputs,
//! and can never produce any side-effects.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`PureSet`].
pub trait Spec: 'static {
    /// The type used to refer to the registers.
    type RegisterId: core::fmt::Debug;

    /// The constant value type supported by this instruction set.
    type ConstValue: core::fmt::Debug;
}

/// An unary operation.
#[derive(Debug)]
pub struct UnaryOp<RegisterId> {
    /// The register to store the result of the operation at.
    pub dst: RegisterId,

    /// The register that contains the operand value.
    pub src: RegisterId,
}

/// A binary operation.
#[derive(Debug)]
pub struct BinaryOp<RegisterId> {
    /// The register to store the result of the operation at.
    pub dst: RegisterId,

    /// The register that contains the first operand.
    pub a: RegisterId,

    /// The register that contains the second operand.
    pub b: RegisterId,
}

/// The kind of binary operation to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOpKind {
    /// Add two values together.
    Add,

    /// Subtract one value from another.
    Sub,

    /// Multiply two values together.
    Mul,

    /// Divide one value by another.
    Div,

    /// Floor-divide one value by another.
    FloorDiv,

    /// Compute one value modulo another.
    Mod,

    /// Compare two values for equality.
    Eq,

    /// Compare two values for inequality.
    Ne,

    /// Compare whether one value is less than another.
    Lt,

    /// Compare whether one value is less than or equal to another.
    Le,

    /// Compare whether one value is greater than another.
    Gt,

    /// Compare whether one value is greater than or equal to another.
    Ge,

    /// Test whether the left operand is contained in the right operand.
    In,

    /// Test whether the left operand is not contained in the right operand.
    NotIn,

    /// Apply Python-style logical `and` to two values.
    And,

    /// Apply Python-style logical `or` to two values.
    Or,
}

/// The kind of unary operation to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOpKind {
    /// Negate a value.
    Neg,

    /// Apply Python-style logical `not` to a value.
    Not,
}

/// One dictionary entry.
#[derive(Debug)]
pub struct DictEntry<RegisterId> {
    /// The register that contains the entry key.
    pub key: RegisterId,

    /// The register that contains the entry value.
    pub value: RegisterId,
}

/// Pure instructions set.
#[derive_where(Debug)]
pub enum PureSet<Spec: self::Spec> {
    /// Load a constant value into the register.
    LoadConst {
        /// The resiter in the current frame to store the value at.
        dst: Spec::RegisterId,

        /// The value to store.
        value: Spec::ConstValue,
    },

    /// Copy a value from one register into another register.
    Copy {
        /// The register to store the copied value at.
        dst: Spec::RegisterId,

        /// The register that contains the value to copy.
        src: Spec::RegisterId,
    },

    /// Apply a binary operation to two values.
    Binary {
        /// The binary operation kind to apply.
        kind: BinaryOpKind,

        /// The registers used by the binary operation.
        op: BinaryOp<Spec::RegisterId>,
    },

    /// Apply a unary operation to a value.
    Unary {
        /// The unary operation kind to apply.
        kind: UnaryOpKind,

        /// The registers used by the unary operation.
        op: UnaryOp<Spec::RegisterId>,
    },

    /// Compute the length of a resolved container value.
    Length {
        /// The register to store the resulting length at.
        dst: Spec::RegisterId,

        /// The register that contains the value to measure.
        src: Spec::RegisterId,
    },

    /// Resolve an indexed access from an object and index value.
    Index {
        /// The register to store the result at.
        dst: Spec::RegisterId,

        /// The register containing the indexed object.
        object: Spec::RegisterId,

        /// The register containing the index value.
        index: Spec::RegisterId,
    },

    /// Resolve an attribute access from an object and attribute name.
    Dot {
        /// The register to store the result at.
        dst: Spec::RegisterId,

        /// The register containing the accessed object.
        object: Spec::RegisterId,

        /// The attribute name to read from the object.
        attribute: String,
    },

    /// Build a list value from resolved registers.
    MakeList {
        /// The register to store the resulting list at.
        dst: Spec::RegisterId,

        /// The registers to read list elements from in order.
        items: Vec<Spec::RegisterId>,
    },

    /// Append a single item onto an existing list value.
    ///
    /// Equivalent to `dst = list + [item]`, but emitted as a single
    /// instruction so per-iteration list growth (spreads, comprehensions)
    /// does not pay the cost of a throwaway one-element `MakeList` plus a
    /// `Binary(Add)`. Callers commonly pass `dst == list` to grow a list in
    /// place, but the variant accepts any destination register.
    ListAppend {
        /// The register to store the grown list at.
        dst: Spec::RegisterId,

        /// The register containing the existing list value.
        list: Spec::RegisterId,

        /// The register containing the item to append.
        item: Spec::RegisterId,
    },

    /// Build a dictionary value from resolved key and value registers.
    MakeDict {
        /// The register to store the resulting dictionary at.
        dst: Spec::RegisterId,

        /// The dictionary entries to read in source order.
        entries: Vec<DictEntry<Spec::RegisterId>>,
    },
}

impl core::fmt::Display for BinaryOpKind {
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

impl core::fmt::Display for UnaryOpKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::Neg => "-",
            Self::Not => "not",
        })
    }
}
