//! A generic coalescing primitive: many producers each `submit` one item and
//! await its individual result, while a single background batcher groups the
//! pending items into batches and hands each batch to a caller-supplied flush
//! closure.
//!
//! It comes in two modes, differing only in how a batch is prepared and how
//! its results are delivered — everything else (the handle, the bounded
//! intake, the accumulate-by-size-or-delay loop, shutdown) is shared:
//!
//! - [`write_batcher`] — **positional**. Each submitted item is distinct and
//!   goes to exactly one waiter; the flush closure sees every item and
//!   returns one output per item, one-to-one. Good for batched writes
//!   (snapshots, inserts, upserts).
//! - [`read_batcher`] — **deduplicating** (a "DataLoader"). Producers submit
//!   keys; if several ask for the same key in one window the flush closure is
//!   called with that key once and its single value is cloned back to every
//!   waiter. Good for batched loads. This is why a read key must be [`Hash`]
//!   + [`Eq`] and its value [`Clone`], where a write needs neither.
//!
//! The crate is deliberately domain-agnostic. It knows nothing about
//! databases, backends, or any project trait — the only thing that touches
//! the outside world is the flush closure the caller provides. In both modes
//! that closure is `FnMut(NEVec<In>) -> impl Future<Output = NEVec<Out>>`,
//! returning one output per input **positionally**; a closure that returns a
//! different count than it was given is a programming error and panics the
//! batcher.
//!
//! # Lifecycle
//!
//! Each constructor returns a ([`BatcherHandle`], `impl Future`) pair. The
//! future is the batcher task — it is **not** spawned; the caller owns it. It
//! resolves on either of two triggers:
//!
//! - **All handles dropped** — the intake channel closes; the batcher
//!   flushes whatever remains and resolves.
//! - **The `shutdown` future resolves** — the batcher closes the intake
//!   (further [`submit`](BatcherHandle::submit)s return [`Closed`]), drains
//!   and flushes the still-buffered items, then resolves. Shutdown is
//!   observed between batches, so a batch already accumulating completes
//!   within [`Policy::max_delay`] of the signal. Pass [`std::future::pending`]
//!   when only drop-driven shutdown is wanted.

#![warn(missing_docs)]

mod strategy;

use std::hash::Hash;
use std::num::NonZeroUsize;

use nonempty_collections::NEVec;
use tokio::sync::{mpsc, oneshot};
use waymark_nonzero_duration::NonZeroDuration;

use self::strategy::{BatchStrategy, Deduplicated, Positional};

/// Controls when the batcher stops accumulating and flushes a batch.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// Flush once this many items are buffered. For [`read_batcher`] these
    /// are counted *before* deduplication.
    pub max_batch: NonZeroUsize,

    /// Flush at most this long after the batch's first item arrived, even if
    /// [`max_batch`](Policy::max_batch) was not reached. Non-zero: a zero
    /// window would flush after every first item and defeat batching.
    pub max_delay: NonZeroDuration,
}

/// An item and the waiter to hand its output back to.
type Job<In, Out> = (In, oneshot::Sender<Out>);

/// Error returned by [`BatcherHandle::submit`] when the batcher can no longer
/// produce an output — it has stopped (its task ended or was dropped), so the
/// submitted item will never be flushed.
#[derive(Debug, thiserror::Error)]
#[error("batcher is closed")]
pub struct Closed;

/// A handle to a batcher: cloneable and shared by every producer. The same
/// type serves both [`write_batcher`] and [`read_batcher`].
///
/// Dropping every clone closes the intake channel and lets the batcher
/// drain-and-exit.
pub struct BatcherHandle<In, Out> {
    tx: mpsc::Sender<Job<In, Out>>,
}

// Derived `Clone` would demand `In: Clone` / `Out: Clone`; the handle is just
// a channel sender, so clone it directly.
impl<In, Out> Clone for BatcherHandle<In, Out> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<In, Out> BatcherHandle<In, Out> {
    /// Enqueue one item (or key) and await the output the flush closure
    /// produces for it.
    ///
    /// Awaits intake capacity first, so a burst of producers naturally
    /// backpressures instead of growing memory without bound.
    ///
    /// Returns [`Closed`] if the batcher is gone and the item can never be
    /// flushed.
    pub async fn submit(&self, item: In) -> Result<Out, Closed> {
        let (out_tx, out_rx) = oneshot::channel();
        self.tx.send((item, out_tx)).await.map_err(|_| Closed)?;
        out_rx.await.map_err(|_| Closed)
    }
}

/// Create a **positional** batcher: a handle to submit items through, and the
/// batcher future to drive.
///
/// `flush_fn` sees every submitted item and must return one output per item,
/// in the same order. See the crate docs for the shutdown triggers.
pub fn write_batcher<In, Out, FlushFn, Fut, Shutdown>(
    policy: Policy,
    flush_fn: FlushFn,
    shutdown: Shutdown,
) -> (BatcherHandle<In, Out>, impl Future<Output = ()>)
where
    FlushFn: FnMut(NEVec<In>) -> Fut,
    Fut: Future<Output = NEVec<Out>>,
    Shutdown: Future<Output = ()>,
{
    build::<In, Out, Positional, _, _, _>(policy, flush_fn, shutdown)
}

/// Create a **deduplicating** batcher (a DataLoader): a handle to submit keys
/// through, and the batcher future to drive.
///
/// `flush_fn` is invoked with the non-empty, deduplicated keys of a window
/// and must return one value per unique key, in the same order; that value is
/// cloned back to every producer that submitted the key. See the crate docs
/// for the shutdown triggers.
pub fn read_batcher<Key, Value, FlushFn, Fut, Shutdown>(
    policy: Policy,
    flush_fn: FlushFn,
    shutdown: Shutdown,
) -> (BatcherHandle<Key, Value>, impl Future<Output = ()>)
where
    Key: Hash + Eq + Clone,
    Value: Clone,
    FlushFn: FnMut(NEVec<Key>) -> Fut,
    Fut: Future<Output = NEVec<Value>>,
    Shutdown: Future<Output = ()>,
{
    build::<Key, Value, Deduplicated, _, _, _>(policy, flush_fn, shutdown)
}

/// Shared wiring: bound intake channel, then the strategy-parameterized loop.
fn build<In, Out, Strategy, FlushFn, Fut, Shutdown>(
    policy: Policy,
    flush_fn: FlushFn,
    shutdown: Shutdown,
) -> (BatcherHandle<In, Out>, impl Future<Output = ()>)
where
    Strategy: BatchStrategy<In, Out>,
    FlushFn: FnMut(NEVec<In>) -> Fut,
    Fut: Future<Output = NEVec<Out>>,
    Shutdown: Future<Output = ()>,
{
    // A few batches' worth of slack: enough that producers rarely block on
    // the send while a flush is in flight, small enough to bound memory.
    let capacity = policy.max_batch.get().saturating_mul(4);
    let (tx, rx) = mpsc::channel(capacity);
    (
        BatcherHandle { tx },
        run::<In, Out, Strategy, _, _, _>(rx, policy, flush_fn, shutdown),
    )
}

/// The shared batcher loop. `Strategy` supplies the two mode-specific steps:
/// turning a window's jobs into the flush input, and delivering the flush
/// output back to the waiters.
async fn run<In, Out, Strategy, FlushFn, Fut, Shutdown>(
    mut rx: mpsc::Receiver<Job<In, Out>>,
    policy: Policy,
    mut flush_fn: FlushFn,
    shutdown: Shutdown,
) where
    Strategy: BatchStrategy<In, Out>,
    FlushFn: FnMut(NEVec<In>) -> Fut,
    Fut: Future<Output = NEVec<Out>>,
    Shutdown: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    // Once shutdown has fired we close the intake and stop watching it —
    // the generic future may not be safe to poll again, and `rx.recv` then
    // simply drains the buffered jobs and ends the loop on its own.
    let mut draining = false;

    loop {
        let first = if draining {
            rx.recv().await
        } else {
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    rx.close();
                    draining = true;
                    rx.recv().await
                }
                job = rx.recv() => job,
            }
        };
        let Some(first) = first else {
            // Channel closed: all handles dropped, or drained after shutdown.
            break;
        };

        // Accumulate up to `max_batch` jobs, or until the delay window —
        // timed from the first job — elapses.  Reserving `max_batch` up
        // front keeps the hot loop at one allocation per batch instead of
        // growth-by-doubling.
        let mut jobs = NEVec::with_capacity(policy.max_batch, first);
        let deadline = tokio::time::sleep(policy.max_delay.get());
        tokio::pin!(deadline);
        while jobs.len().get() < policy.max_batch.get() {
            tokio::select! {
                biased;
                () = &mut deadline => break,
                job = rx.recv() => match job {
                    Some(job) => jobs.push(job),
                    None => break,
                },
            }
        }

        // Mode-specific: build the flush input and the delivery plan.
        let (input, plan) = Strategy::prepare(jobs);
        let expected = input.len();
        let outputs = flush_fn(input).await;
        assert_eq!(
            outputs.len(),
            expected,
            "flush must return exactly one output per input",
        );
        // Mode-specific: route each output to its waiter(s).
        Strategy::deliver(plan, outputs);
    }
}

#[cfg(test)]
mod tests;
