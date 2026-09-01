use waymark_vm_exception_handler::{ExceptionHandler, ExceptionHandlerBlock};
use waymark_vm_runtime_exception::Exception;

use crate::RegisterId;

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct ExceptionScope<StateId> {
    handlers: ExceptionHandlerBlock<StateId, RegisterId>,
    finally_state: Option<StateId>,
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum PendingTransfer<StateId, Value> {
    Jump { depth: usize, state: StateId },
    Return(Value),
    Raise(Exception<Value>),
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum UnwindEntry<StateId, Value> {
    ExceptionScope(ExceptionScope<StateId>),
    Pending(PendingTransfer<StateId, Value>),
}

/// The next action selected while unwinding a frame.
pub enum UnwindOutcome<StateId, Value> {
    /// Enter a bytecode state.
    State(StateId),

    /// Enter a matching exception handler with the raised exception.
    Handle {
        /// Handler selected for the exception.
        handler: ExceptionHandler<StateId, RegisterId>,

        /// Exception delivered to the handler.
        exception: Exception<Value>,
    },

    /// Return a value from the frame.
    Return(Value),

    /// Exit the frame with an unhandled exception.
    Unhandled(Exception<Value>),
}

/// Frame-local stack of exception scopes and suspended control transfers.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UnwindStack<StateId, Value>(Vec<UnwindEntry<StateId, Value>>);

impl<StateId, Value> Default for UnwindStack<StateId, Value> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<StateId, Value> UnwindStack<StateId, Value> {
    /// Creates an empty unwind stack.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pushes one exception scope as the new innermost scope.
    pub fn push_exception_handlers(
        &mut self,
        handlers: ExceptionHandlerBlock<StateId, RegisterId>,
        finally_state: Option<StateId>,
    ) {
        self.0.push(UnwindEntry::ExceptionScope(ExceptionScope {
            handlers,
            finally_state,
        }));
    }

    /// Leaves scopes above `depth`, running their finalizers before `state`.
    pub fn jump(
        &mut self,
        depth: usize,
        state: StateId,
    ) -> Result<UnwindOutcome<StateId, Value>, UnwindDepthError> {
        self.begin_control_transfer(PendingTransfer::Jump { depth, state })
    }

    /// Leaves every scope, running finalizers before returning `value`.
    pub fn return_value(&mut self, value: Value) -> UnwindOutcome<StateId, Value> {
        self.begin_control_transfer(PendingTransfer::Return(value))
            .expect("returning to depth zero cannot underflow")
    }

    /// Raises `exception`, running crossed finalizers before handling it.
    pub fn raise(&mut self, exception: Exception<Value>) -> UnwindOutcome<StateId, Value> {
        for entry_index in (0..self.0.len()).rev() {
            let UnwindEntry::ExceptionScope(scope) = &mut self.0[entry_index] else {
                continue;
            };

            if let Some(handler_index) = scope
                .handlers
                .iter()
                .position(|handler| handler.matches(&exception.type_id))
            {
                let handler = scope.handlers.remove(handler_index);
                let finally_state = scope.finally_state.take();
                self.0.truncate(entry_index);
                if let Some(finally_state) = finally_state {
                    self.push_exception_handlers(Vec::new(), Some(finally_state));
                }
                return UnwindOutcome::Handle { handler, exception };
            }

            if let Some(finally_state) = scope.finally_state.take() {
                self.0.truncate(entry_index);
                self.0
                    .push(UnwindEntry::Pending(PendingTransfer::Raise(exception)));
                return UnwindOutcome::State(finally_state);
            }
        }

        self.0.clear();
        UnwindOutcome::Unhandled(exception)
    }

    /// Resumes the transfer suspended by the current finalizer.
    pub fn continue_unwind(
        &mut self,
    ) -> Result<UnwindOutcome<StateId, Value>, ContinueUnwindError> {
        let Some(UnwindEntry::Pending(_)) = self.0.last() else {
            return Err(ContinueUnwindError);
        };
        let Some(UnwindEntry::Pending(transfer)) = self.0.pop() else {
            unreachable!("checked pending transfer should still exist");
        };
        Ok(match transfer {
            PendingTransfer::Raise(exception) => self.raise(exception),
            transfer => self
                .begin_control_transfer(transfer)
                .expect("a previously validated unwind depth should remain valid"),
        })
    }

    fn begin_control_transfer(
        &mut self,
        transfer: PendingTransfer<StateId, Value>,
    ) -> Result<UnwindOutcome<StateId, Value>, UnwindDepthError> {
        let depth = match &transfer {
            PendingTransfer::Jump { depth, .. } => *depth,
            PendingTransfer::Return(_) => 0,
            PendingTransfer::Raise(_) => unreachable!("raise uses exception matching"),
        };
        if depth > self.0.len() {
            return Err(UnwindDepthError {
                target: depth,
                active: self.0.len(),
            });
        }

        for entry_index in (depth..self.0.len()).rev() {
            let UnwindEntry::ExceptionScope(scope) = &mut self.0[entry_index] else {
                continue;
            };
            let Some(finally_state) = scope.finally_state.take() else {
                continue;
            };
            self.0.truncate(entry_index);
            self.0.push(UnwindEntry::Pending(transfer));
            return Ok(UnwindOutcome::State(finally_state));
        }

        self.0.truncate(depth);
        Ok(match transfer {
            PendingTransfer::Jump { state, .. } => UnwindOutcome::State(state),
            PendingTransfer::Return(value) => UnwindOutcome::Return(value),
            PendingTransfer::Raise(_) => unreachable!("raise uses exception matching"),
        })
    }
}

/// An error returned when bytecode refers to a nonexistent unwind depth.
#[derive(Debug, thiserror::Error)]
#[error("unwind depth {target} exceeds active depth {active}")]
pub struct UnwindDepthError {
    /// Requested unwind depth.
    pub target: usize,

    /// Active unwind depth.
    pub active: usize,
}

/// An error returned when no suspended transfer is available to resume.
#[derive(Debug, thiserror::Error)]
#[error("no control transfer is awaiting finalization")]
pub struct ContinueUnwindError;
