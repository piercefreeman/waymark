use derive_where::derive_where;

use crate::{Continuation, ResumeWithValue, SelectStateClaim};

/// A party waiting on a promise to settle.
#[derive_where(
    Debug;
    FunctionId, StateId, Value, RaisedException,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>,
)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            FunctionId: serde::Serialize,
            StateId: serde::Serialize,
            Value: serde::Serialize,
            RaisedException: serde::Serialize,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
        ",
        deserialize = "
            FunctionId: serde::Deserialize<'de>,
            StateId: serde::Deserialize<'de>,
            Value: serde::Deserialize<'de>,
            RaisedException: serde::Deserialize<'de>,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Deserialize<'de>,
        ",
    ))
)]
pub enum PromiseWaiter<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// A continuation to resume when the promise settles.
    Await(Continuation<FunctionId, StateId, Value, RaisedException, ResumeWithValue>),

    /// The promise is an arm of a select: its settlement claims the select
    /// continuation and delivers the outcome to the arm's target.
    ///
    /// A settlement of either kind fires the arm the same way - resolution
    /// delivers the value to the arm's target, rejection raises - resuming
    /// at the arm's resume state either way. The first arm to fire claims
    /// the select; later firings find it already claimed and are inert.
    Select(SelectStateClaim<StateId>),
}
