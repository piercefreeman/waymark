use crate::{Continuation, ResumeWithValue, SelectStateClaim};

/// A party waiting on a promise to settle.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PromiseWaiter<FunctionId, StateId, Value> {
    /// A continuation to resume when the promise settles.
    Await(Continuation<FunctionId, StateId, Value, ResumeWithValue>),

    /// The promise is an arm of a select: its settlement claims the select
    /// continuation and delivers the outcome to the arm's target.
    ///
    /// A settlement of either kind fires the arm the same way - resolution
    /// delivers the value to the arm's target, rejection raises - resuming
    /// at the arm's resume state either way. The first arm to fire claims
    /// the select; later firings find it already claimed and are inert.
    Select(SelectStateClaim<StateId>),
}
