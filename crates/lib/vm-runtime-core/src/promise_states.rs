use index_type::{IndexType, typed_vec::TypedVec};

use crate::{Continuation, PromiseState, ResolvingAlreadyResolvedPromiseError};

/// Index of a [`PromiseState`] in the [`PromiseStates`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
pub struct PromiseStateId(pub usize);

pub struct PromiseStates<Value>(TypedVec<PromiseStateId, PromiseState<Value>>);

impl<Value> Default for PromiseStates<Value> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<Value> PromiseStates<Value> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn prepare(&mut self) -> PromiseStateId {
        self.0.push(PromiseState::Waiting(Vec::new()))
    }
}

impl<Value> std::ops::Index<PromiseStateId> for PromiseStates<Value> {
    type Output = PromiseState<Value>;

    fn index(&self, index: PromiseStateId) -> &Self::Output {
        &self.0[index]
    }
}

impl<Value> std::ops::IndexMut<PromiseStateId> for PromiseStates<Value> {
    fn index_mut(&mut self, index: PromiseStateId) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<Value> PromiseStates<Value> {
    pub fn resolve(
        &mut self,
        promise_id: PromiseStateId,
        value: Value,
    ) -> Result<Vec<Continuation<Value>>, ResolvingAlreadyResolvedPromiseError> {
        self.0[promise_id].resolve(value)
    }
}
