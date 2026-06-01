use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{ExceptionHandler, Registers};

/// An error returned when popping more exception-handler blocks than are active.
#[derive(Debug, thiserror::Error)]
#[error("exception handler stack underflow")]
pub struct PopExceptionHandlersError;

/// A frame shape used in runtime.
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
    pub exception_handler_blocks: Vec<Vec<ExceptionHandler<StateId>>>,

    /// The kind of the frame.
    pub kind: FrameKind,
}

/// The kind of a frame.
#[derive(Debug)]
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

impl<FunctionId, StateId, Value> Frame<FunctionId, StateId, Value> {
    /// Pushes one exception-handler block as the new innermost active scope.
    pub fn push_exception_handlers(&mut self, handlers: Vec<ExceptionHandler<StateId>>) {
        self.exception_handler_blocks.push(handlers);
    }

    /// Pops `count` innermost exception-handler blocks.
    pub fn pop_exception_handlers(
        &mut self,
        count: usize,
    ) -> Result<(), PopExceptionHandlersError> {
        if count > self.exception_handler_blocks.len() {
            return Err(PopExceptionHandlersError);
        }

        let keep = self.exception_handler_blocks.len() - count;
        self.exception_handler_blocks.truncate(keep);
        Ok(())
    }
}

impl<FunctionId, StateId, Value> Frame<FunctionId, StateId, Value>
where
    StateId: Clone,
{
    /// Returns the innermost matching handler and unwinds active handler blocks
    /// to the surrounding scope of that handler.
    pub fn take_matching_exception_handler(
        &mut self,
        type_id: &str,
    ) -> Option<ExceptionHandler<StateId>> {
        let (block_index, handler) = self
            .exception_handler_blocks
            .iter()
            .enumerate()
            .rev()
            .find_map(|(block_index, handlers)| {
                handlers
                    .iter()
                    .find(|handler| handler.matches(type_id))
                    .cloned()
                    .map(|handler| (block_index, handler))
            })?;

        self.exception_handler_blocks.truncate(block_index);
        Some(handler)
    }
}
