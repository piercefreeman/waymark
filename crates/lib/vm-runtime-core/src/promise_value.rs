use crate::PromiseStateId;

#[derive(Clone, Debug)]
pub enum Promise<Value> {
    Pending(PromiseStateId),
    Resolved(Value),
}

#[derive(Debug, thiserror::Error)]
#[error("an unresolved async value is used where a resolved value is expected")]
pub struct UnresolvedPromiseError {
    pub promise_state_id: PromiseStateId,
}

impl<Value> Promise<Value> {
    pub fn require_resolved(self) -> Result<Value, UnresolvedPromiseError> {
        match self {
            Promise::Pending(promise_state_id) => Err(UnresolvedPromiseError { promise_state_id }),
            Promise::Resolved(value) => Ok(value),
        }
    }

    pub fn require_resolved_ref(&self) -> Result<&Value, UnresolvedPromiseError> {
        match self {
            Promise::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
            Promise::Resolved(value) => Ok(value),
        }
    }

    pub fn require_resolved_mut(&mut self) -> Result<&mut Value, UnresolvedPromiseError> {
        match self {
            Promise::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
            Promise::Resolved(value) => Ok(value),
        }
    }
}
