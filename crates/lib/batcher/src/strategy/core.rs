//! The mode seam: how a batch's jobs become the flush input, and how the
//! flush output routes back to the waiters.

use nonempty_collections::NEVec;

use crate::Job;

/// How a batch's jobs are turned into the flush input and how the flush
/// output is delivered back to the waiters. The shared loop knows nothing
/// beyond these two steps.
pub trait BatchStrategy<In, Out> {
    /// Bookkeeping retained between building the flush input and delivering
    /// the flush output.
    type Plan;

    /// Reduce a window's jobs to the flush input (the items or keys handed to
    /// the flush closure) plus a plan for routing the outputs back.
    fn prepare(&self, jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan);

    /// Route each output — positionally aligned with the flush input — to the
    /// waiter(s) it belongs to.
    fn deliver(&self, plan: Self::Plan, outputs: NEVec<Out>);
}
