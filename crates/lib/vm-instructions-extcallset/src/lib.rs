//! The "extcall" instruction set for the VM.
//!
//! Responsible for representing asynchronous operations that suspend execution.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`ExtCallSet`].
pub trait Spec: 'static {
    /// The type used to refer to the registers.
    type RegisterId: core::fmt::Debug;

    /// The type used to refer to the executable function sub-states.
    type StateId: core::fmt::Debug;

    /// The type used to refer to an action.
    type ActionRef: core::fmt::Debug;
}

/// The external-call instructions set.
#[derive_where(Debug)]
pub enum ExtCallSet<Spec: self::Spec> {
    /// Start an action call execution.
    ///
    /// External calls are always asynchronous by nature.
    ActionCall {
        /// The register in the current frame to assign the promise for this
        /// new call completion.
        dst: Spec::RegisterId,

        /// The action to invoke.
        action_ref: Spec::ActionRef,

        /// The registers in the current frame to take the arguments to pass to
        /// the extcall from.
        args: Vec<Spec::RegisterId>,

        /// The state to resume the execution from after invoking the extcall.
        resume: Spec::StateId,
    },
}
