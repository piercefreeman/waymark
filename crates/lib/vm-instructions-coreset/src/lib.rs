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

    /// Suspend the execution until the first of several promises settles,
    /// resuming through that promise's arm.
    ///
    /// The arms are scanned in the listed order: an arm whose source holds
    /// a ready value or an already-settled promise is taken immediately.
    /// Otherwise the frame is kept aside and resumed - exactly once - by
    /// whichever arm settles first. Either settlement kind takes an arm
    /// the same way: a resolution delivers the value to the arm's `dst`,
    /// a rejection raises - resuming from the arm's `resume` state either
    /// way. The losing arms are inert.
    Select {
        /// The arms to select over.
        ///
        /// Must not be empty.
        arms: Vec<SelectArm<Spec::RegisterId, Spec::StateId>>,
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

/// One arm of a [`CoreSet::Select`] instruction.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectArm<RegisterId, StateId> {
    /// The register containing the promise this arm watches.
    pub src: RegisterId,

    /// The register to store this arm's resolved value at.
    pub dst: RegisterId,

    /// Resume from this state when this arm is taken.
    pub resume: StateId,
}
