//! The two mode-specific steps that distinguish a positional (write) batcher
//! from a deduplicating (read) batcher, behind one trait the shared loop
//! drives.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use tokio::sync::oneshot;

use crate::Job;

/// How a batch's jobs are turned into the flush input and how the flush
/// output is delivered back to the waiters. The shared loop knows nothing
/// beyond these two steps.
pub(crate) trait BatchStrategy<In, Out> {
    /// Bookkeeping retained between building the flush input and delivering
    /// the flush output.
    type Plan;

    /// Reduce a window's jobs to the flush input (the items or keys handed to
    /// the flush closure) plus a plan for routing the outputs back.
    fn prepare(jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan);

    /// Route each output — positionally aligned with the flush input — to the
    /// waiter(s) it belongs to.
    fn deliver(plan: Self::Plan, outputs: NEVec<Out>);
}

/// Positional, one-to-one: every submitted item is flushed and its single
/// output goes to that item's one waiter. No key bounds, no cloning.
pub(crate) struct Positional;

impl<In, Out> BatchStrategy<In, Out> for Positional {
    type Plan = NEVec<oneshot::Sender<Out>>;

    fn prepare(jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan) {
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

    fn deliver(waiters: Self::Plan, outputs: NEVec<Out>) {
        for (output, waiter) in outputs.into_iter().zip(waiters) {
            // A dropped waiter (producer gave up / was cancelled) is fine;
            // its output is simply discarded.
            let _ = waiter.send(output);
        }
    }
}

/// Deduplicating: keys seen more than once in a window are flushed once, and
/// the single value is cloned back to every waiter that submitted the key.
pub(crate) struct Deduplicated;

impl<In, Out> BatchStrategy<In, Out> for Deduplicated
where
    In: Hash + Eq + Clone,
    Out: Clone,
{
    type Plan = (Vec<In>, HashMap<In, Vec<oneshot::Sender<Out>>>);

    fn prepare(jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan) {
        // One entry per key in first-seen order, gathering the waiters that
        // asked for it.
        let mut order: Vec<In> = Vec::new();
        let mut waiters: HashMap<In, Vec<oneshot::Sender<Out>>> = HashMap::new();
        for (key, waiter) in jobs {
            match waiters.entry(key.clone()) {
                Entry::Vacant(vacant) => {
                    order.push(key);
                    vacant.insert(vec![waiter]);
                }
                Entry::Occupied(mut occupied) => occupied.get_mut().push(waiter),
            }
        }
        // `order` is non-empty: the first job's key is always present.
        let unique = NEVec::try_from_vec(order.clone()).expect("at least the first key is present");
        (unique, (order, waiters))
    }

    fn deliver((order, mut waiters): Self::Plan, outputs: NEVec<Out>) {
        for (key, value) in order.into_iter().zip(outputs) {
            let key_waiters = waiters
                .remove(&key)
                .expect("every unique key has at least one waiter");
            for waiter in key_waiters {
                let _ = waiter.send(value.clone());
            }
        }
    }
}
