use waymark_vm_exception_handler::{ExceptionHandler, ExceptionHandlerBlock};

use crate::RegisterId;

/// A state and the frame unwind depth expected when entering it.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StateTarget<StateId> {
    /// State to enter.
    pub state: StateId,

    /// Number of unwind entries surrounding the state.
    pub unwind_depth: usize,
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct StateCall<StateId> {
    remaining: Vec<StateTarget<StateId>>,
    return_to: StateTarget<StateId>,
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum UnwindEntry<StateId> {
    ExceptionHandlers(ExceptionHandlerBlock<StateId, RegisterId>),
    StateCall(StateCall<StateId>),
}

/// Frame-local stack for exception scopes and shared-state returns.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UnwindStack<StateId>(Vec<UnwindEntry<StateId>>);

impl<StateId> Default for UnwindStack<StateId> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<StateId> UnwindStack<StateId> {
    /// Creates an empty unwind stack.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pushes one exception-handler block as the new innermost scope.
    pub fn push_exception_handlers(
        &mut self,
        handlers: ExceptionHandlerBlock<StateId, RegisterId>,
    ) {
        self.0.push(UnwindEntry::ExceptionHandlers(handlers));
    }

    /// Discards entries above `depth`.
    pub fn unwind_to(&mut self, depth: usize) -> Result<(), UnwindDepthError> {
        if depth > self.0.len() {
            return Err(UnwindDepthError {
                target: depth,
                active: self.0.len(),
            });
        }
        self.0.truncate(depth);
        Ok(())
    }

    /// Starts a state chain and enters its first target or return state.
    pub fn call_states(
        &mut self,
        mut targets: Vec<StateTarget<StateId>>,
        return_to: StateTarget<StateId>,
    ) -> Result<StateId, UnwindDepthError> {
        targets.reverse();
        let Some(target) = targets.pop() else {
            self.unwind_to(return_to.unwind_depth)?;
            return Ok(return_to.state);
        };
        self.unwind_to(target.unwind_depth)?;
        self.0.push(UnwindEntry::StateCall(StateCall {
            remaining: targets,
            return_to,
        }));
        Ok(target.state)
    }

    /// Advances the current state chain or returns to its caller.
    pub fn return_state(&mut self) -> Result<StateId, ReturnStateError> {
        let Some(UnwindEntry::StateCall(_)) = self.0.last() else {
            return Err(ReturnStateError::MissingCall);
        };
        let Some(UnwindEntry::StateCall(mut call)) = self.0.pop() else {
            unreachable!("checked state call should still exist");
        };
        if let Some(target) = call.remaining.pop() {
            self.unwind_to(target.unwind_depth)
                .map_err(ReturnStateError::Unwind)?;
            self.0.push(UnwindEntry::StateCall(call));
            return Ok(target.state);
        }
        self.unwind_to(call.return_to.unwind_depth)
            .map_err(ReturnStateError::Unwind)?;
        Ok(call.return_to.state)
    }

    /// Returns the innermost matching handler and unwinds through its scope.
    pub fn take_matching(
        &mut self,
        type_id: &str,
    ) -> Option<ExceptionHandler<StateId, RegisterId>> {
        for entry_index in (0..self.0.len()).rev() {
            let UnwindEntry::ExceptionHandlers(handlers) = &mut self.0[entry_index] else {
                continue;
            };
            if let Some(handler_index) =
                handlers.iter().position(|handler| handler.matches(type_id))
            {
                let handler = handlers.remove(handler_index);
                self.0.truncate(entry_index);
                return Some(handler);
            }
        }
        None
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

/// An error returned when no shared state is active.
#[derive(Debug, thiserror::Error)]
pub enum ReturnStateError {
    /// The top unwind entry is not a shared state call.
    #[error("no shared state is active")]
    MissingCall,

    /// The return target refers to a nonexistent unwind depth.
    #[error("return target: {0}")]
    Unwind(#[source] UnwindDepthError),
}
