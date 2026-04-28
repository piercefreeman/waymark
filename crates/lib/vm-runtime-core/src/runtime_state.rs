use std::collections::VecDeque;

use crate::{Frame, Promise, PromiseStateId, PromiseStates, ResolvingAlreadyResolvedPromiseError};

pub struct RuntimeState<Value> {
    // TODO: replace with a more restricted interface
    pub ready: VecDeque<Frame<Promise<Value>>>,
    pub promise_states: PromiseStates<Promise<Value>>,
}

impl<Value> RuntimeState<Value>
where
    Value: Clone,
{
    /// Provide an async computation value for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn resolve_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        val: Promise<Value>,
    ) -> Result<(), ResolvingAlreadyResolvedPromiseError> {
        let continuations = self.promise_states.resolve(promise_state_id, val.clone())?;

        for continuation in continuations {
            let frame = continuation.resume(val.clone());
            self.ready.push_back(frame);
        }

        Ok(())
    }
}
