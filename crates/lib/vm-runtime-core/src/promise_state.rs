use crate::Continuation;

#[derive(Debug, thiserror::Error)]
#[error("resolving an already resolved promise")]
pub struct ResolvingAlreadyResolvedPromiseError;

pub enum PromiseState<Value> {
    Waiting(Vec<Continuation<Value>>),
    Ready(Value),
}

impl<Value> PromiseState<Value> {
    pub fn resolve(
        &mut self,
        value: Value,
    ) -> Result<Vec<crate::Continuation<Value>>, ResolvingAlreadyResolvedPromiseError> {
        let ready = Self::Ready(value);
        let replaced = std::mem::replace(self, ready);
        let continuations = match replaced {
            PromiseState::Waiting(continuations) => continuations,
            PromiseState::Ready(old_value) => {
                // Replace the value back and we don't want inoncsistencies.
                *self = Self::Ready(old_value);
                return Err(ResolvingAlreadyResolvedPromiseError);
            }
        };
        Ok(continuations)
    }
}
