//! The "core" instruction set for the VM.
//!
//! Responsible for representing function calls, returns, awaits, and control
//! flow.
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
}

/// The core instructions set.
#[derive_where(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            Spec::RegisterId: serde::Serialize,
            Spec::FunctionId: serde::Serialize,
            Spec::StateId: serde::Serialize,
        ",
        deserialize = "
            Spec::RegisterId: serde::Deserialize<'de>,
            Spec::FunctionId: serde::Deserialize<'de>,
            Spec::StateId: serde::Deserialize<'de>,
        ",
    ))
)]
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

    /// Suspend the execution until a promise is resolved.
    Await {
        /// The register containing the promise to suspend on.
        dst: Spec::RegisterId,

        /// The register to store the resolved promise value at.
        src: Spec::RegisterId,

        /// Resume from the this state when the promise resolves.
        resume: Spec::StateId,
    },

    /// Create a race promise that resolves with the index of the first
    /// source to settle.
    ///
    /// The sources are scanned in the listed order: a source that holds
    /// a ready value or an already-settled promise wins outright and no
    /// race promise is allocated. Otherwise a race promise is created and
    /// every source gets a race arm that resolves it upon settlement -
    /// the first arm to fire wins, and a settlement of either kind fires
    /// the arm the same way.
    ///
    /// This instruction does not suspend the execution: awaiting the race
    /// promise - and then the winning source - is up to the subsequent
    /// instructions.
    Race {
        /// The register in the current frame to store the race promise at.
        dst: Spec::RegisterId,

        /// The registers holding the sources to race.
        ///
        /// Must not be empty.
        srcs: Vec<Spec::RegisterId>,
    },

    /// Push one exception-handler block as the new innermost active scope.
    PushExceptionHandlers {
        /// Handlers to activate for subsequent execution in this frame.
        handlers:
            Vec<waymark_vm_exception_handler::ExceptionHandler<Spec::StateId, Spec::RegisterId>>,
    },

    /// Pop `count` innermost exception-handler blocks.
    PopExceptionHandlers {
        /// Number of blocks to remove.
        count: usize,
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

    /// Raise the exception value stored in a register.
    Raise {
        /// The register that stores the exception value to raise.
        src: Spec::RegisterId,
    },

    /// Return the value at the given registry.
    Return {
        /// The register in the current from to take the return value from.
        src: Spec::RegisterId,
    },
}
