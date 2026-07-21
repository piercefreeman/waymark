use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{Continuation, ResumeWithValue};

/// A party waiting on a promise to settle.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Waiter<FunctionId, StateId, Value> {
    /// A suspended frame to resume when the promise settles.
    Continuation(Continuation<FunctionId, StateId, Value, ResumeWithValue>),

    /// The promise is an arm of a race: its settlement resolves the race
    /// promise with the pre-built arm-index value.
    ///
    /// A settlement of either kind fires the arm the same way - the race
    /// promise only ever learns *which* arm settled first, never how.
    /// The first arm to fire wins; later firings find the race promise
    /// already settled and are inert.
    RaceArm {
        /// The race promise to resolve when this arm settles.
        race: PromiseStateId,

        /// The value to resolve the race promise with - the arm's index,
        /// pre-constructed at arm installation time.
        resolution: Value,
    },
}
