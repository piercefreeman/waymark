//! The "exc" instruction set for the VM.
//!
//! Responsible for exceptions support.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`ExcSet`].
pub trait Spec: 'static {
    /// The type used to refer to the registers.
    type RegisterId: core::fmt::Debug;
}

/// The exception instructions set.
#[derive_where(Debug)]
pub enum ExcSet<Spec: self::Spec> {
    /// Checks whether a value is an exception with an optional type filter.
    IsException {
        /// Destination register for the boolean result of the type check.
        dst: Spec::RegisterId,

        /// Register holding the value to inspect.
        src: Spec::RegisterId,

        /// Optional register holding the exception type id to compare against.
        ///
        /// When absent, the instruction checks only whether `src` is any
        /// exception value.
        exception_type_id: Option<Spec::RegisterId>,
    },

    /// Extracts the details payload from an exception value.
    ExceptionDetails {
        /// Destination register for the extracted details value.
        dst: Spec::RegisterId,

        /// Register holding the exception value.
        src: Spec::RegisterId,
    },
}
