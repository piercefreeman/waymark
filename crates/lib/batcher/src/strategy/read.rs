//! The deduplicating (read) strategy.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

use nonempty_collections::NEVec;
use tokio::sync::oneshot;

use crate::Job;

/// Deduplicating: keys seen more than once in a window are flushed once, and
/// the single value is cloned back to every waiter that submitted the key.
pub struct BatchStrategy;

impl<In, Out> super::core::BatchStrategy<In, Out> for BatchStrategy
where
    In: Hash + Eq + Clone,
    Out: Clone,
{
    type Plan = (Vec<In>, HashMap<In, Vec<oneshot::Sender<Out>>>);

    fn prepare(&self, jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan) {
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

    fn deliver(&self, (order, mut waiters): Self::Plan, outputs: NEVec<Out>) {
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

#[cfg(test)]
mod tests;
