/// A state and the exception-handler depth expected when entering it.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StateTarget<StateId> {
    /// State to enter.
    pub state: StateId,

    /// Exception-handler depth surrounding the state.
    pub exception_handler_depth: usize,
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct StateCall<StateId> {
    remaining: Vec<StateTarget<StateId>>,
    return_to: StateTarget<StateId>,
    active_exception_handler_depth: usize,
}

/// Frame-local return stack for shared state calls.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StateCalls<StateId>(Vec<StateCall<StateId>>);

impl<StateId> Default for StateCalls<StateId> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<StateId> StateCalls<StateId> {
    /// Creates an empty state-call stack.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a state chain after discarding calls deeper than `pending_depth`.
    pub fn push(
        &mut self,
        pending_depth: usize,
        mut targets: Vec<StateTarget<StateId>>,
        return_to: StateTarget<StateId>,
    ) -> Result<StateTarget<StateId>, StateCallDepthError> {
        self.truncate(pending_depth)?;
        targets.reverse();
        let Some(target) = targets.pop() else {
            return Ok(return_to);
        };
        self.0.push(StateCall {
            remaining: targets,
            return_to,
            active_exception_handler_depth: target.exception_handler_depth,
        });
        Ok(target)
    }

    /// Advances the current state chain or returns to its caller.
    pub fn pop(&mut self) -> Result<StateTarget<StateId>, ReturnStateError> {
        let Some(call) = self.0.last_mut() else {
            return Err(ReturnStateError);
        };
        if let Some(target) = call.remaining.pop() {
            call.active_exception_handler_depth = target.exception_handler_depth;
            return Ok(target);
        }
        Ok(self
            .0
            .pop()
            .expect("active state call should exist")
            .return_to)
    }

    /// Discards calls deeper than `pending_depth`.
    pub fn truncate(&mut self, pending_depth: usize) -> Result<(), StateCallDepthError> {
        if pending_depth > self.0.len() {
            return Err(StateCallDepthError);
        }
        self.0.truncate(pending_depth);
        Ok(())
    }

    /// Discards state calls exited by an exception handled at `handler_depth`.
    pub fn unwind_to_handler_depth(&mut self, handler_depth: usize) {
        while self
            .0
            .last()
            .is_some_and(|call| call.active_exception_handler_depth > handler_depth)
        {
            self.0.pop();
        }
    }

    /// Discards every pending state call.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

/// An error returned when bytecode refers to a nonexistent state-call depth.
#[derive(Debug, thiserror::Error)]
#[error("state-call stack underflow")]
pub struct StateCallDepthError;

/// An error returned when no shared state is active.
#[derive(Debug, thiserror::Error)]
#[error("no shared state is active")]
pub struct ReturnStateError;
