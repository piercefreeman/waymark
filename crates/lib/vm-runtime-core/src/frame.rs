use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{ExceptionHandlers, Registers};

/// A frame shape used in runtime.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Frame<FunctionId, StateId, Value> {
    /// A function this frame is executing.
    pub func: FunctionId,

    /// A function sub-state this frame is executing.
    pub state: StateId,

    /// Registers that hold values for this frame.
    pub regs: Registers<Value>,

    /// Raised exception associated with this frame.
    pub exception: Option<Exception<Value>>,

    /// Exception-handler blocks active for this frame from outermost to innermost.
    pub exception_handler_blocks: ExceptionHandlers<StateId>,

    /// The kind of the frame.
    pub kind: FrameKind,
}

/// The kind of a frame.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameKind {
    /// Top level frame.
    ///
    /// Represents a function that the execution of the runtime
    /// began with.
    /// A return from the top-level frame completes the whole runtime execution.
    TopLevel,

    /// A function call frame.
    ///
    /// Represents an function that was invoked from somewhere and that has
    /// as associated promise to fulful upon the function return.
    FnCall {
        /// The promise to resolve when this frame returns.
        ret: PromiseStateId,
    },
}
