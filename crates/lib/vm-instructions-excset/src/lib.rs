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

    /// Checks whether a value is an exception that should bubble.
    ShouldBubble {
        /// Destination register for the bubbling flag.
        dst: Spec::RegisterId,

        /// Register holding the value to inspect.
        src: Spec::RegisterId,
    },

    /// Marks an exception as handled so it no longer bubbles automatically.
    CatchException {
        /// Register holding the exception value to mark as caught.
        src: Spec::RegisterId,
    },

    /// Raises an exception value, transferring control to the current state's
    /// handler slot if one is active, or unwinding the frame otherwise.
    ///
    /// Raise forces the exception value's bubbling flag to `true` before
    /// propagation, so explicit re-raise always bubbles.
    ///
    /// The runtime consults the *current state's* handler slot (set by the
    /// compiler when lowering try/except). If a slot is present, the runtime
    /// copies `src` into the slot's exception register and jumps to the
    /// dispatcher state. If no slot is present, the frame unwinds with `src`
    /// as the propagated exception value, and the caller's awaiting state
    /// observes it via auto-raise on `Await` resume.
    Raise {
        /// Register holding the exception value to raise.
        src: Spec::RegisterId,
    },
}
