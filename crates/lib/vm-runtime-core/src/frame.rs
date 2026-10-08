use derive_where::derive_where;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{ExceptionHandlers, Registers};

/// A frame shape used in runtime.
///
/// `RaisedException` is the raised exception type of the instruction set executing
/// the frame: a pending one lives on the frame until the unwind delivers
/// it, and the handler blocks list the patterns it matches.
#[derive_where(
    Debug;
    FunctionId, StateId, Value, RaisedException,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>,
)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            FunctionId: serde::Serialize,
            StateId: serde::Serialize,
            Value: serde::Serialize,
            RaisedException: serde::Serialize,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
        ",
        deserialize = "
            FunctionId: serde::Deserialize<'de>,
            StateId: serde::Deserialize<'de>,
            Value: serde::Deserialize<'de>,
            RaisedException: serde::Deserialize<'de>,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Deserialize<'de>,
        ",
    ))
)]
pub struct Frame<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// A function this frame is executing.
    pub func: FunctionId,

    /// A function sub-state this frame is executing.
    pub state: StateId,

    /// Registers that hold values for this frame.
    pub regs: Registers<Value>,

    /// The raised exception pending on this frame, until the unwind
    /// delivers it.
    pub exception: Option<RaisedException>,

    /// Exception-handler blocks active for this frame from outermost to innermost.
    pub exception_handler_blocks:
        ExceptionHandlers<StateId, waymark_vm_runtime_exception::MatchPatternOf<RaisedException>>,

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

impl<FunctionId, StateId, Value, RaisedException>
    waymark_vm_interpreter_composite_core::DetectStateSwitch
    for Frame<FunctionId, StateId, Value, RaisedException>
where
    StateId: Copy + PartialEq,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    type StateToken = StateId;

    fn capture_state_token(&self) -> Self::StateToken {
        self.state
    }

    fn state_switched(&self, token: &Self::StateToken) -> bool {
        self.state != *token
    }
}

impl<FunctionId, StateId, Value, RaisedException> Frame<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// Raise a runtime exception on this frame.
    ///
    /// If an exception is already pending on the frame, keeps the pending
    /// exception and discards the provided one.
    pub fn raise_exception(&mut self, exception: RaisedException) {
        self.exception.get_or_insert(exception);
    }

    /// Raise the exception a runtime error lifts to on this frame.
    ///
    /// How the error renders as an exception is the raised exception type's
    /// own business - hence the `From` bound.
    ///
    /// See [`Frame::raise_exception`].
    pub fn raise_exception_from_error<Error>(&mut self, error: Error)
    where
        RaisedException: From<Error>,
    {
        self.raise_exception(error.into());
    }
}
