//! The positional (write) strategy.

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use tokio::sync::oneshot;

use crate::Job;

/// Positional, one-to-one: every submitted item is flushed and its single
/// output goes to that item's one waiter. No key bounds, no cloning.
pub struct BatchStrategy;

impl<In, Out> super::core::BatchStrategy<In, Out> for BatchStrategy {
    type Plan = NEVec<oneshot::Sender<Out>>;

    fn prepare(&self, jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan) {
        let capacity = jobs.len();
        let ((first_item, first_waiter), rest) = jobs.into_nonempty_iter().next();
        let mut items = NEVec::with_capacity(capacity, first_item);
        let mut waiters = NEVec::with_capacity(capacity, first_waiter);
        for (item, waiter) in rest {
            items.push(item);
            waiters.push(waiter);
        }
        (items, waiters)
    }

    fn deliver(&self, waiters: Self::Plan, outputs: NEVec<Out>) {
        for (output, waiter) in outputs.into_iter().zip(waiters) {
            // A dropped waiter (producer gave up / was cancelled) is fine;
            // its output is simply discarded.
            let _ = waiter.send(output);
        }
    }
}

#[cfg(test)]
mod tests;
