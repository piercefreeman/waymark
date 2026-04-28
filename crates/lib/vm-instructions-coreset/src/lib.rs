//! The "core" instruction set for the VM.
//!
//! Responsible for representing function calls, returns, awaits, control flow
//! and extcalls.
//!
//! Minimal core functionality of the VM.

#![warn(missing_docs)]

use derive_where::derive_where;

/// The spec for required data types for the [`CoreSet`].
pub trait Spec: 'static {
    /// The type used to refer to the registers.
    type RegisterId: core::fmt::Debug;

    /// The type used to refer to the executable functions.
    type FunctionId: core::fmt::Debug;

    /// The type used to refer to the executable function sub-states.
    type StateId: core::fmt::Debug;

    /// The type used to refer to the extcalls.
    type ExtCallId: core::fmt::Debug;
}

/// The core instructions set.
#[derive_where(Debug)]
pub enum CoreSet<Spec: self::Spec> {
    /// Call a function.
    ///
    /// Creates a new frame and populates its registers with the arguments.
    Call {
        /// The resiter in the current frame to assign the promise for this
        /// new call completion.
        dst: Spec::RegisterId,

        /// The function to call.
        function_id: Spec::FunctionId,

        /// The registers in the current frame to take the arguments to pass to
        /// the function from.
        args: Vec<Spec::RegisterId>,
    },

    /// Start an extcall execution.
    ///
    /// External calls are always asynchronous by nature.
    ExtCall {
        /// The resiter in the current frame to assign the promise for this
        /// new call completion.
        dst: Spec::RegisterId,

        /// The ID of the extcall to invoke.
        extcall_id: Spec::ExtCallId,

        /// The registers in the current frame to take the arguments to pass to
        /// the extcall from.
        args: Vec<Spec::RegisterId>,

        /// The state to resume the execution from after invoking the extcall.
        resume: Spec::StateId,
    },

    /// Suspend the execution until a promise is resolved.
    Await {
        /// The register containing the promise to suspend on.
        dst: Spec::RegisterId,

        /// The register to store the resolved promise value at.
        src: Spec::RegisterId,

        /// Resume from the this state when the promise resolves.
        resume: Spec::StateId,
    },

    /// Jump to the specified state.
    Jump {
        /// The state to jump to.
        target_state: Spec::StateId,
    },

    /// Jump to the specified state if the cond is true.
    JumpIf {
        /// The state to jump to.
        target_state: Spec::StateId,

        /// The register containing the value that will be evaluated as the
        /// condition for the jump.
        cond: Spec::RegisterId,
    },

    /// Return the value at the given registry.
    Return {
        /// The register in the current from to take the return value from.
        src: Spec::RegisterId,
    },
}
