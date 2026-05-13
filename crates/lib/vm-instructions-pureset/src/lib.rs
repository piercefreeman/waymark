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

    /// Add two values together.
    Add(BinaryOp<Spec::RegisterId>),

    /// Subtract one value from another.
    Sub(BinaryOp<Spec::RegisterId>),

    /// Multiply two values together.
    Mul(BinaryOp<Spec::RegisterId>),

    /// Divide one value by another.
    Div(BinaryOp<Spec::RegisterId>),

    /// Floor-divide one value by another.
    FloorDiv(BinaryOp<Spec::RegisterId>),

    /// Compute one value modulo another.
    Mod(BinaryOp<Spec::RegisterId>),

    /// Compare two values for equality.
    Eq(BinaryOp<Spec::RegisterId>),

    /// Compare two values for inequality.
    Ne(BinaryOp<Spec::RegisterId>),

    /// Compare whether one value is less than another.
    Lt(BinaryOp<Spec::RegisterId>),

    /// Compare whether one value is less than or equal to another.
    Le(BinaryOp<Spec::RegisterId>),

    /// Compare whether one value is greater than another.
    Gt(BinaryOp<Spec::RegisterId>),

    /// Compare whether one value is greater than or equal to another.
    Ge(BinaryOp<Spec::RegisterId>),

    /// Test whether the left operand is contained in the right operand.
    In(BinaryOp<Spec::RegisterId>),

    /// Test whether the left operand is not contained in the right operand.
    NotIn(BinaryOp<Spec::RegisterId>),

    /// Apply Python-style logical `and` to two values.
    And(BinaryOp<Spec::RegisterId>),

    /// Apply Python-style logical `or` to two values.
    Or(BinaryOp<Spec::RegisterId>),

    /// Negate a value.
    Neg(UnaryOp<Spec::RegisterId>),

    /// Apply Python-style logical `not` to a value.
    Not(UnaryOp<Spec::RegisterId>),

    /// Build a list value from resolved registers.
    MakeList {
        /// The register to store the resulting list at.
        dst: Spec::RegisterId,

        /// The registers to read list elements from in order.
        items: Vec<Spec::RegisterId>,
    },
}
