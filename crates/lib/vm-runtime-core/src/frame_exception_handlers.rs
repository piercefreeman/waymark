use waymark_vm_exception_handler::{ExceptionHandler, ExceptionHandlerBlock};

use crate::RegisterId;

/// An error returned when popping more exception-handler blocks than are active.
#[derive(Debug, thiserror::Error)]
#[error("exception handler stack underflow")]
pub struct PopExceptionHandlersError;

/// Frame-local stack of active exception-handler blocks.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExceptionHandlers<StateId>(Vec<ExceptionHandlerBlock<StateId, RegisterId>>);

impl<StateId> Default for ExceptionHandlers<StateId> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<StateId> ExceptionHandlers<StateId> {
    /// Creates an empty exception-handler stack.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pushes one exception-handler block as the new innermost active scope.
    pub fn push(&mut self, handlers: ExceptionHandlerBlock<StateId, RegisterId>) {
        self.0.push(handlers);
    }

    /// Pops `count` innermost exception-handler blocks.
    pub fn pop(&mut self, count: usize) -> Result<(), PopExceptionHandlersError> {
        if count > self.0.len() {
            return Err(PopExceptionHandlersError);
        }

        let keep = self.0.len() - count;
        self.0.truncate(keep);
        Ok(())
    }

    /// Returns the innermost matching handler and unwinds active handler blocks
    /// to the surrounding scope of that handler.
    pub fn take_matching(
        &mut self,
        type_id: &str,
    ) -> Option<ExceptionHandler<StateId, RegisterId>> {
        for block_index in (0..self.0.len()).rev() {
            if let Some(handler_index) = self.0[block_index]
                .iter()
                .position(|handler| handler.matches(type_id))
            {
                let handler = self.0[block_index].remove(handler_index);
                self.0.truncate(block_index);
                return Some(handler);
            }
        }

        None
    }
}
