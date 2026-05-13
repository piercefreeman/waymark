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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOpKind {
    /// Negate a value.
    Neg,

    /// Apply Python-style logical `not` to a value.
    Not,
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

    /// Build a list value from resolved registers.
    MakeList {
        /// The register to store the resulting list at.
        dst: Spec::RegisterId,

        /// The registers to read list elements from in order.
        items: Vec<Spec::RegisterId>,
    },
}
