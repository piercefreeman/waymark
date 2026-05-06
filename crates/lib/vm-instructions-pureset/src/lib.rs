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

    /// Build a list value from resolved registers.
    MakeList {
        /// The register to store the resulting list at.
        dst: Spec::RegisterId,

        /// The registers to read list elements from in order.
        items: Vec<Spec::RegisterId>,
    },
}
