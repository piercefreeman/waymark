//! A push-and-forget batching primitive: producers `push` items —
//! synchronously, never waiting, with nothing coming back — while
//! background flushers hand batches to a caller-supplied [`Flusher`].
//! When flushing cannot keep up, items are dropped and counted. Use it
//! for data that must never slow its producer.
//!
//! It is the lossy sibling of `waymark-batcher` — the submit-and-await
//! counterpart for correctness state, where producers block on intake
//! and receive their item's flush output.
//!
//! # The swapchain
//!
//! [`Policy::buffers`] `Vec<T>`s of [`Policy::max_batch`] capacity are
//! allocated at construction and reused forever. One is *filling*: `push`
//! appends to it under a short mutex, and once it turns full (or
//! [`Policy::max_delay`] old) it is exchanged for an empty buffer — a
//! header swap — and flushed, up to [`Policy::flushers`] concurrently.
//! Items are dropped only when every buffer is out being flushed.
//!
//! # Lifecycle
//!
//! [`lossy_batcher`] returns a ([`BatcherHandle`], task future) pair; the
//! caller spawns the task. The task ends when `shutdown` resolves: the
//! filling buffer goes out one last time (best effort), pending flushes
//! finish, and every later push counts as `closed`. Dropping handles does
//! not end the task.

#![warn(missing_docs)]

mod swapchain;

use std::num::NonZeroUsize;
use std::sync::Arc;

use nonempty_collections::{NESlice, NEVec};
use tokio::sync::mpsc;
use waymark_nonzero_duration::NonZeroDuration;

/// Controls buffering, the flush triggers, and flush concurrency.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// Number of pre-allocated buffers. Standby capacity while flushes are
    /// slow is `buffers × max_batch` items.
    pub buffers: NonZeroUsize,

    /// Capacity of one buffer; a full buffer is swapped out and flushed.
    pub max_batch: NonZeroUsize,

    /// A non-empty filling buffer whose first item is this old is swapped
    /// out even if not full.
    pub max_delay: NonZeroDuration,

    /// How many full buffers are flushed concurrently; more than
    /// `buffers − 1` is refused by [`validate`](Self::validate): the
    /// loops beyond that can never all be busy. Hides flush latency — the
    /// store-facing flush rate is set by push rate / `max_batch`, not by
    /// this.
    pub flushers: NonZeroUsize,
}

impl Policy {
    /// The only way to a [`ValidPolicy`]: [`TooManyFlushers`] unless
    /// `flushers ≤ buffers − 1`.
    pub fn validate(self) -> Result<ValidPolicy, TooManyFlushers> {
        if self.flushers.get() >= self.buffers.get() {
            return Err(TooManyFlushers {
                flushers: self.flushers,
                buffers: self.buffers,
            });
        }
        Ok(ValidPolicy(self))
    }
}

/// A [`Policy`] that passed [`Policy::validate`] — the only form
/// [`lossy_batcher`] accepts.
#[derive(Debug, Clone, Copy)]
pub struct ValidPolicy(Policy);

/// Setup error: the policy asks for more concurrent flushes than there are
/// buffers to hold them — `flushers` loops beyond `buffers − 1` could never
/// all be busy.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("flushers ({flushers}) must be at most buffers - 1 ({buffers} buffers)")]
pub struct TooManyFlushers {
    /// The requested flush concurrency.
    pub flushers: NonZeroUsize,

    /// The requested buffer count.
    pub buffers: NonZeroUsize,
}

/// The consumer of a batcher's batches.
///
/// The batch is lent: the buffer behind it is cleared and reused after the
/// call, so a flusher that needs owned data clones explicitly. The future
/// is `Send` so the batcher task can be spawned; `flush` takes `&self` so
/// one flusher serves all [`Policy::flushers`] loops.
pub trait Flusher<T> {
    /// A failure drops the batch: counted, warned, never retried.
    type Error: std::fmt::Display;

    /// Write one batch out.
    fn flush<'a>(
        &'a self,
        batch: NESlice<'a, T>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

impl<T, Flusher: self::Flusher<T>> self::Flusher<T> for Arc<Flusher> {
    type Error = Flusher::Error;

    fn flush<'a>(
        &'a self,
        batch: NESlice<'a, T>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        Flusher::flush(self, batch)
    }
}

/// Saturate an item count into a counter increment.
fn to_u64_saturating(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// This instance's counters, bound to their labels at construction.
struct Counters {
    pub dropped_full: metrics::Counter,
    pub dropped_closed: metrics::Counter,
    pub dropped_flush_failed: metrics::Counter,
    pub flushed: metrics::Counter,
}

/// What the handles and the batcher task share: the mechanism
/// ([`swapchain::Swapchain`]), the timer signal, and the accounting
/// ([`Counters`]) — tied together by the `record_*` methods, which map
/// what the mechanism reports onto the counters and the signal.
struct Shared<T> {
    pub swapchain: swapchain::Swapchain<T>,

    /// Wakes the delay timer when a first item enters an empty buffer.
    pub first_push: tokio::sync::Notify,

    pub counters: Counters,
}

impl<T> Shared<T> {
    /// Map a push outcome onto the counters and the timer signal.
    fn record_push(&self, outcome: swapchain::PushOutcome) {
        if outcome.first_push {
            self.first_push.notify_one();
        }
        if outcome.discarded_full > 0 {
            self.counters
                .dropped_full
                .increment(to_u64_saturating(outcome.discarded_full));
        }
        if outcome.discarded_closed > 0 {
            self.counters
                .dropped_closed
                .increment(to_u64_saturating(outcome.discarded_closed));
        }
    }

    /// Map a swap outcome onto the counters.
    fn record_swap(&self, outcome: swapchain::SwapOutcome) {
        match outcome {
            swapchain::SwapOutcome::Sent => {}
            swapchain::SwapOutcome::DroppedFull(discarded) => {
                self.counters
                    .dropped_full
                    .increment(to_u64_saturating(discarded));
            }
        }
    }
}

/// A handle to a lossy batcher: cloneable and shared by every producer.
pub struct BatcherHandle<T> {
    shared: Arc<Shared<T>>,
}

// Derived `Clone` would demand `T: Clone`; the handle is just an `Arc`.
impl<T> Clone for BatcherHandle<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> BatcherHandle<T> {
    /// Hand one item to the batcher: synchronous, never waits, never fails.
    /// The item is either flushed later or dropped and counted.
    pub fn push(&self, item: T) {
        let outcome = self.shared.swapchain.push(item);
        self.shared.record_push(outcome);
    }

    /// [`push`](Self::push) every item, taking the lock once.
    pub fn push_many(&self, items: impl IntoIterator<Item = T>) {
        let outcome = self.shared.swapchain.push_many(items);
        self.shared.record_push(outcome);
    }
}

/// Create a lossy batcher: a handle to push items through, and the batcher
/// task future for the caller to spawn.
///
/// The [`Policy::flushers`] loops share `flusher`. `name` labels this
/// instance's metrics, which are registered here against the recorder
/// installed at call time.
pub fn lossy_batcher<T, Flusher, Shutdown>(
    name: &'static str,
    policy: ValidPolicy,
    flusher: Flusher,
    shutdown: Shutdown,
) -> (BatcherHandle<T>, impl Future<Output = ()>)
where
    Flusher: self::Flusher<T>,
    Shutdown: Future<Output = ()>,
{
    let ValidPolicy(policy) = policy;

    let (swapchain, full_rx) = swapchain::Swapchain::new(policy.buffers, policy.max_batch);
    let dropped = |reason: &'static str| {
        metrics::counter!(
            "waymark_lossy_batcher_dropped_total",
            "batcher" => name,
            "reason" => reason,
        )
    };
    let shared = Arc::new(Shared {
        swapchain,
        first_push: tokio::sync::Notify::new(),
        counters: Counters {
            dropped_full: dropped("full"),
            dropped_closed: dropped("closed"),
            dropped_flush_failed: dropped("flush_failed"),
            flushed: metrics::counter!(
                "waymark_lossy_batcher_flushed_total",
                "batcher" => name,
            ),
        },
    });

    let handle = BatcherHandle {
        shared: Arc::clone(&shared),
    };
    let task = run(shared, full_rx, policy, name, flusher, shutdown);
    (handle, task)
}

/// The batcher task: the delay timer, the flush loops, and the shutdown
/// sequence.
async fn run<T, Flusher, Shutdown>(
    shared: Arc<Shared<T>>,
    full_rx: mpsc::Receiver<NEVec<T>>,
    policy: Policy,
    name: &'static str,
    flusher: Flusher,
    shutdown: Shutdown,
) where
    Flusher: self::Flusher<T>,
    Shutdown: Future<Output = ()>,
{
    // The flush loops share the receiver through an async mutex, locked
    // only while waiting for a buffer — never across a flush — so multiple
    // flushers genuinely overlap.
    let full_rx = tokio::sync::Mutex::new(full_rx);
    let mut flushers = std::pin::pin!(futures_util::future::join_all(
        (0..policy.flushers.get()).map(|_| flush_loop(&shared, &full_rx, name, &flusher)),
    ));
    // The timer lives only for the select: its block ends, it drops.
    {
        let timer = std::pin::pin!(timer(&shared, policy.max_delay));
        tokio::select! {
            biased;
            () = shutdown => {}
            never = timer => match never {},
            _ = &mut flushers => {
                unreachable!("full_tx lives in the swapchain, so the flushers cannot see the channel close before shutdown")
            }
        }
    }

    // Close the intake: every later push is refused and counted `closed`;
    // a final non-empty filling buffer goes out best effort (counted
    // `full` when no buffer is free, like any other swap).
    if let Some(outcome) = shared.swapchain.close() {
        shared.record_swap(outcome);
    }
    // Let the flushers drain what is buffered, then stop.
    full_rx.lock().await.close();
    flushers.await;
}

/// Swaps out a non-empty filling buffer once its first item is `max_delay`
/// old. Sleeps against the first item's exact deadline; while the buffer is
/// empty, parks until [`Shared::first_push`].
async fn timer<T>(shared: &Shared<T>, max_delay: NonZeroDuration) -> std::convert::Infallible {
    loop {
        match shared.swapchain.first_push_at() {
            None => shared.first_push.notified().await,
            Some(first_push_at) => {
                tokio::time::sleep_until(first_push_at + max_delay.get()).await;
                if let Some(outcome) = shared.swapchain.swap_overdue(max_delay) {
                    shared.record_swap(outcome);
                }
            }
        }
    }
}

/// One flush loop: take a full buffer, flush it (lent), return it empty to
/// the free pool. Ends when the channel is closed and drained.
async fn flush_loop<T, Flusher>(
    shared: &Shared<T>,
    full_rx: &tokio::sync::Mutex<mpsc::Receiver<NEVec<T>>>,
    name: &'static str,
    flusher: &Flusher,
) where
    Flusher: self::Flusher<T>,
{
    loop {
        let full = { full_rx.lock().await.recv().await };
        let Some(full) = full else { break };
        let len = to_u64_saturating(full.len().get());
        match flusher.flush(full.as_nonempty_slice()).await {
            Ok(()) => shared.counters.flushed.increment(len),
            Err(error) => {
                tracing::warn!(%error, batcher = name, items = len, "flush failed, batch dropped");
                shared.counters.dropped_flush_failed.increment(len);
            }
        }
        shared.swapchain.recycle(Vec::from(full));
    }
}

#[cfg(test)]
mod tests;
