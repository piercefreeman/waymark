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

impl<FunctionId, StateId, Value> waymark_vm_interpreter_composite_core::DetectStateSwitch
    for Frame<FunctionId, StateId, Value>
where
    StateId: Copy + PartialEq,
{
    type StateToken = StateId;

    fn capture_state_token(&self) -> Self::StateToken {
        self.state
    }

    fn state_switched(&self, token: &Self::StateToken) -> bool {
        self.state != *token
    }
}

impl<FunctionId, StateId, Value> Frame<FunctionId, StateId, Value> {
    /// Raise a runtime exception on this frame.
    ///
    /// If an exception is already pending on the frame, keeps the pending
    /// exception and discards the provided one.
    pub fn raise_exception(&mut self, exception: Exception<Value>) {
        self.exception.get_or_insert(exception);
    }

    /// Raise a typed runtime exception on this frame.
    ///
    /// The operations supply the conversion of the typed error into the
    /// value domain.
    ///
    /// See [`Frame::raise_exception`].
    pub fn raise_typed_exception<Operations, TypedException>(&mut self, exception: TypedException)
    where
        TypedException: waymark_vm_runtime_exception::TypedException,
        Value: waymark_vm_runtime_value::RootValueAccess<RootValue = Value>,
        Operations: waymark_vm_runtime_exception::ExceptionFromIntermediate<
                TypedException::IntermediateDetails,
                Value,
            >,
    {
        self.raise_exception(Operations::from_intermediate_exception(
            exception.into_intermediate_exception(),
        ));
    }
}
