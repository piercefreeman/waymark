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
    Add {
        /// The register to store the addition result at.
        dst: Spec::RegisterId,

        /// The register that contains the first value for
        /// the addition operation.
        a: Spec::RegisterId,

        /// The register that contains the second value for
        /// the addition operation.
        b: Spec::RegisterId,
    },

    /// Subtract one value from another.
    Sub {
        /// The register to store the subtraction result at.
        dst: Spec::RegisterId,

        /// The register that contains the first value for
        /// the subtraction operation.
        a: Spec::RegisterId,

        /// The register that contains the second value for
        /// the subtraction operation.
        b: Spec::RegisterId,
    },

    /// Multiply two values together.
    Mul {
        /// The register to store the multiplication result at.
        dst: Spec::RegisterId,

        /// The register that contains the first value for
        /// the multiplication operation.
        a: Spec::RegisterId,

        /// The register that contains the second value for
        /// the multiplication operation.
        b: Spec::RegisterId,
    },

    /// Divide one value by another.
    Div {
        /// The register to store the division result at.
        dst: Spec::RegisterId,

        /// The register that contains the dividend.
        a: Spec::RegisterId,

        /// The register that contains the divisor.
        b: Spec::RegisterId,
    },

    /// Floor-divide one value by another.
    FloorDiv {
        /// The register to store the floor-division result at.
        dst: Spec::RegisterId,

        /// The register that contains the dividend.
        a: Spec::RegisterId,

        /// The register that contains the divisor.
        b: Spec::RegisterId,
    },

    /// Compute one value modulo another.
    Mod {
        /// The register to store the modulo result at.
        dst: Spec::RegisterId,

        /// The register that contains the dividend.
        a: Spec::RegisterId,

        /// The register that contains the divisor.
        b: Spec::RegisterId,
    },

    /// Compare two values for equality.
    Eq {
        /// The register to store the equality result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Compare two values for inequality.
    Ne {
        /// The register to store the inequality result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Compare whether one value is less than another.
    Lt {
        /// The register to store the comparison result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Compare whether one value is less than or equal to another.
    Le {
        /// The register to store the comparison result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Compare whether one value is greater than another.
    Gt {
        /// The register to store the comparison result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Compare whether one value is greater than or equal to another.
    Ge {
        /// The register to store the comparison result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Test whether the left operand is contained in the right operand.
    In {
        /// The register to store the membership result at.
        dst: Spec::RegisterId,

        /// The register that contains the candidate value.
        a: Spec::RegisterId,

        /// The register that contains the container value.
        b: Spec::RegisterId,
    },

    /// Test whether the left operand is not contained in the right operand.
    NotIn {
        /// The register to store the membership result at.
        dst: Spec::RegisterId,

        /// The register that contains the candidate value.
        a: Spec::RegisterId,

        /// The register that contains the container value.
        b: Spec::RegisterId,
    },

    /// Apply Python-style logical `and` to two values.
    And {
        /// The register to store the logical result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Apply Python-style logical `or` to two values.
    Or {
        /// The register to store the logical result at.
        dst: Spec::RegisterId,

        /// The register that contains the left operand.
        a: Spec::RegisterId,

        /// The register that contains the right operand.
        b: Spec::RegisterId,
    },

    /// Negate a value.
    Neg {
        /// The register to store the negated result at.
        dst: Spec::RegisterId,

        /// The register that contains the operand value.
        src: Spec::RegisterId,
    },

    /// Apply Python-style logical `not` to a value.
    Not {
        /// The register to store the logical result at.
        dst: Spec::RegisterId,

        /// The register that contains the operand value.
        src: Spec::RegisterId,
    },

    /// Build a list value from resolved registers.
    MakeList {
        /// The register to store the resulting list at.
        dst: Spec::RegisterId,

        /// The registers to read list elements from in order.
        items: Vec<Spec::RegisterId>,
    },
}
