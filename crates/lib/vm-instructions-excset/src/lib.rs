//! The "exception" instruction set for the VM.
//!
//! Responsible for raising exceptions and for the handler blocks that
//! catch them. The VM has no exception shape of its own: what a handler
//! lists and what the compiler embeds for its own raises are the spec's
//! const types, converted into the interpreter's runtime ones at execution
//! - the same distinction a const value bears to a runtime value.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`ExcSet`].
pub trait Spec: 'static {
    /// The type used to refer to the registers.
    type RegisterId: core::fmt::Debug;

    /// The type used to refer to the executable function sub-states.
    type StateId: core::fmt::Debug;

    /// The exception embedded in the bytecode by [`ExcSet::RaiseConst`].
    type ConstException: core::fmt::Debug;

    /// The pattern a handler lists in the bytecode, as a constant.
    type ConstExceptionPattern: core::fmt::Debug;
}

/// The exception handler record as the bytecode carries it: listing the
/// spec's const pattern.
pub type ConstExceptionHandlerFor<Spec> = waymark_vm_exception_handler::ExceptionHandler<
    <Spec as self::Spec>::StateId,
    <Spec as self::Spec>::RegisterId,
    <Spec as self::Spec>::ConstExceptionPattern,
>;

/// The exception instructions set.
#[derive_where(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            Spec::RegisterId: serde::Serialize,
            Spec::StateId: serde::Serialize,
            Spec::ConstException: serde::Serialize,
            Spec::ConstExceptionPattern: serde::Serialize,
        ",
        deserialize = "
            Spec::RegisterId: serde::Deserialize<'de>,
            Spec::StateId: serde::Deserialize<'de>,
            Spec::ConstException: serde::Deserialize<'de>,
            Spec::ConstExceptionPattern: serde::Deserialize<'de>,
        ",
    ))
)]
pub enum ExcSet<Spec: self::Spec> {
    /// Push one exception-handler block as the new innermost active scope.
    PushExceptionHandlers {
        /// Handlers to activate for subsequent execution in this frame.
        handlers: Vec<ConstExceptionHandlerFor<Spec>>,
    },

    /// Pop `count` innermost exception-handler blocks.
    PopExceptionHandlers {
        /// Number of blocks to remove.
        count: usize,
    },

    /// Raise the exception value stored in a register.
    Raise {
        /// The register that stores the exception value to raise.
        src: Spec::RegisterId,
    },

    /// Raise the exception embedded in the bytecode.
    RaiseConst {
        /// The exception to raise.
        exception: Spec::ConstException,
    },
}
