//! A push-and-forget batching primitive: producers `push` items —
//! synchronously, never waiting, with nothing coming back — while
//! background flushers hand batches to a caller-supplied [`Flusher`].
//! When flushing cannot keep up, items are dropped and counted. Use it
//! for data that must never slow its producer.
//!
//! Lossy means this: under contention, when no buffer is free, the full
//! filling buffer is discarded rather than a producer slowed. A batch
//! whose flush fails is dropped as well. Every drop a running batcher
//! makes is counted by its reason. Lossy does not mean work may be
//! discarded to simplify coordination.
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
//! Batches are handed off in push order; with more than one flusher their
//! flushes complete in any order. Items are dropped only when every other
//! buffer is out being flushed: a full filling buffer is discarded, an
//! overdue one waits for a buffer to come back.
//!
//! # Lifecycle
//!
//! [`lossy_batcher`] returns a ([`BatcherHandle`], task future) pair; the
//! caller spawns the task. The task ends when every handle is dropped or
//! when `shutdown` resolves. On the last handle the filling buffer goes
//! out once a buffer is free, since no push can come any more; on
//! `shutdown` it goes out best effort (discarded and counted `full` when
//! no buffer is free). Pending flushes finish, and every later push counts
//! as `closed`. Dropping the task while handles are alive, before the
//! intake closes, loses, uncounted, what is queued and every flush the
//! abort interrupts, and those buffers never come back; a flush already
//! running on another worker may still complete, counted `flushed` or
//! `flush_failed`, and return its buffer. Once the aborted flushers have
//! been dropped — they are tasks, so that waits for the scheduler — a
//! later swap that finds a buffer free finds them gone: the intake
//! closes, and that batch and every later push count as `closed`. Until
//! then a swap can still send into the open channel, and that batch is
//! lost with what is queued. With no buffer free, every full batch counts
//! `full` and the intake stays open. The buffer filling at the drop is
//! counted with the swap that next fills it; it goes uncounted only if it
//! never fills, since the delay timer died with the task. Dropped after
//! the `shutdown` close, the task loses only what is queued and the
//! flushes the abort interrupts: every push already counts `closed`.

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
    /// Number of pre-allocated buffers, at least 2: one fills while the
    /// others stand by. Up to `buffers × max_batch` items are resident
    /// while flushes are slow — one buffer filling, the rest queued or in
    /// flight.
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
    /// The only way to a [`ValidPolicy`]: a [`ValidateError`] unless
    /// `buffers ≥ 2` and `flushers ≤ buffers − 1`.
    pub fn validate(self) -> Result<ValidPolicy, ValidateError> {
        let Some(max_flushers) = NonZeroUsize::new(self.buffers.get() - 1) else {
            return Err(ValidateError::TooFewBuffers {
                buffers: self.buffers,
            });
        };
        if self.flushers > max_flushers {
            return Err(ValidateError::TooManyFlushers {
                flushers: self.flushers,
                buffers: self.buffers,
                max_flushers,
            });
        }
        Ok(ValidPolicy(self))
    }
}

/// A [`Policy`] that passed [`Policy::validate`] — the only form
/// [`lossy_batcher`] accepts.
#[derive(Debug, Clone, Copy)]
pub struct ValidPolicy(Policy);

/// Setup error from [`Policy::validate`].
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ValidateError {
    /// A single buffer leaves nothing to stand by while it is out: every
    /// swap would discard.
    #[error("buffers ({buffers}) must be at least 2: one fills while the others stand by")]
    TooFewBuffers {
        /// The requested buffer count.
        buffers: NonZeroUsize,
    },

    /// The policy asks for more concurrent flushes than there are buffers
    /// to hold them — `flushers` loops beyond `buffers − 1` could never all
    /// be busy.
    #[error(
        "flushers ({flushers}) must be at most {max_flushers} (buffers - 1, with {buffers} buffers)"
    )]
    TooManyFlushers {
        /// The requested flush concurrency.
        flushers: NonZeroUsize,

        /// The requested buffer count.
        buffers: NonZeroUsize,

        /// The bound: `buffers − 1`.
        max_flushers: NonZeroUsize,
    },
}

/// The consumer of a batcher's batches.
///
/// The batch is lent: the buffer behind it is cleared and reused after the
/// call, so a flusher that needs owned data clones explicitly. `flush`
/// takes `&self` so one flusher serves all [`Policy::flushers`] loops, and
/// its future is `Send` so those loops can run as tasks of their own.
pub trait Flusher<T> {
    /// A failure drops the batch: counted, warned, never retried.
    type Error: std::fmt::Display;

    /// Write one batch out.
    fn flush<'a>(
        &'a self,
        batch: NESlice<'a, T>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

impl<T, Flusher> self::Flusher<T> for Arc<Flusher>
where
    Flusher: self::Flusher<T>,
{
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

/// What the handles and the batcher task share: the swapchain, the
/// counters and the signals. The push, swap and recycle outcomes go
/// through the `record_*` methods; the flush loops count their flushes
/// directly; the handle count moves with `Clone` and `Drop`.
struct Shared<T> {
    /// The buffers.
    pub swapchain: swapchain::Swapchain<T>,

    /// Wakes the delay timer when a first item enters an empty buffer.
    pub first_push: tokio::sync::Notify,

    /// Wakes a held delay timer, or the close waiting on the last handle,
    /// when a buffer returns to an empty free buffers pool.
    pub buffer_freed: tokio::sync::Notify,

    /// How many [`BatcherHandle`]s are alive.
    pub handles: std::sync::atomic::AtomicUsize,

    /// Wakes the task when the last handle is dropped.
    pub handles_gone: tokio::sync::Notify,

    /// The metrics.
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
            swapchain::SwapOutcome::DroppedClosed(discarded) => {
                self.counters
                    .dropped_closed
                    .increment(to_u64_saturating(discarded));
            }
        }
    }

    /// Map a recycle outcome onto the timer signal.
    fn record_recycle(&self, outcome: swapchain::RecycleOutcome) {
        if outcome.free_was_empty {
            self.buffer_freed.notify_one();
        }
    }
}

/// A handle to a lossy batcher: cloneable and shared by every producer.
pub struct BatcherHandle<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BatcherHandle<T> {
    fn clone(&self) -> Self {
        self.shared
            .handles
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for BatcherHandle<T> {
    fn drop(&mut self) {
        if self
            .shared
            .handles
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.shared.handles_gone.notify_one();
        }
    }
}

impl<T> BatcherHandle<T> {
    /// Hand one item to the batcher: synchronous, never waits, never fails.
    /// While the batcher task runs, the item is either flushed later or
    /// dropped and counted; for a dropped task see the crate's Lifecycle
    /// section.
    pub fn push(&self, item: T) {
        let outcome = self.shared.swapchain.push(item);
        self.shared.record_push(outcome);
    }

    /// [`push`](Self::push) every item — at least one — taking the lock
    /// once. The iterator runs under that lock, which is not re-entrant:
    /// its items must not push into this batcher.
    ///
    /// # Panics
    ///
    /// A panic in the iterator poisons the lock: every later push on every
    /// handle panics, and the batcher task dies without closing. Hand in
    /// iterators that cannot panic.
    pub fn push_many(&self, items: impl nonempty_collections::IntoNonEmptyIterator<Item = T>) {
        let outcome = self.shared.swapchain.push_many(items);
        self.shared.record_push(outcome);
    }
}

/// Create a lossy batcher: a handle to push items through, and the batcher
/// task future for the caller to spawn.
///
/// The [`Policy::flushers`] loops share `flusher`, each from a task of its
/// own. `name` labels this instance's metrics, which are registered here
/// against the recorder installed at call time.
pub fn lossy_batcher<T, Flusher, Shutdown>(
    name: &'static str,
    policy: ValidPolicy,
    flusher: Flusher,
    shutdown: Shutdown,
) -> (BatcherHandle<T>, impl Future<Output = ()>)
where
    T: Send + 'static,
    Flusher: self::Flusher<T> + Send + Sync + 'static,
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
        buffer_freed: tokio::sync::Notify::new(),
        handles: std::sync::atomic::AtomicUsize::new(1),
        handles_gone: tokio::sync::Notify::new(),
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

/// The batcher task: the delay timer and the close sequence, with the
/// flush loops as tasks of its own.
async fn run<T, Flusher, Shutdown>(
    shared: Arc<Shared<T>>,
    full_rx: mpsc::Receiver<NEVec<T>>,
    policy: Policy,
    name: &'static str,
    flusher: Flusher,
    shutdown: Shutdown,
) where
    T: Send + 'static,
    Flusher: self::Flusher<T> + Send + Sync + 'static,
    Shutdown: Future<Output = ()>,
{
    // The flush loops are tasks of their own, so they run on while this
    // task waits, or is not polled at all, and share the receiver through
    // an async mutex, locked only while waiting for a buffer — never
    // across a flush — so multiple flushers genuinely overlap.
    let mut flushers = {
        let full_rx = Arc::new(tokio::sync::Mutex::new(full_rx));
        let flusher = Arc::new(flusher);
        let mut flushers = tokio::task::JoinSet::new();
        for _ in 0..policy.flushers.get() {
            flushers.spawn({
                let shared = Arc::clone(&shared);
                let full_rx = Arc::clone(&full_rx);
                let flusher = Arc::clone(&flusher);
                async move { flush_loop(&shared, &full_rx, name, &flusher).await }
            });
        }
        flushers
    };
    let mut shutdown = std::pin::pin!(shutdown);
    // The timer lives only for the select: its block ends, it drops.
    let by_shutdown = {
        let timer = std::pin::pin!(timer(&shared, policy.max_delay));
        tokio::select! {
            biased;
            () = &mut shutdown => true,
            () = shared.handles_gone.notified() => false,
            never = timer => match never {},
            // A flush loop ends only after the close, so a join before it
            // is a panic, re-raised here rather than at the end.
            Some(Err(join_error)) = flushers.join_next() => {
                std::panic::resume_unwind(join_error.into_panic())
            }
        }
    };

    // Close the intake: every later push is refused and counted `closed`.
    // On the last handle no push can come any more, so a final non-empty
    // filling buffer waits for a free buffer and goes out; `shutdown`,
    // from the start or while that wait is on, closes best effort (the
    // buffer is discarded and counted `full` when no buffer is free).
    // Closing drops the sender, so the flushers drain what is buffered and
    // end on their own.
    let outcome = if by_shutdown {
        shared.swapchain.close()
    } else {
        loop {
            if let Ok(outcome) = shared.swapchain.try_close() {
                break outcome;
            }
            tokio::select! {
                biased;
                () = &mut shutdown => break shared.swapchain.close(),
                () = shared.buffer_freed.notified() => {}
                Some(Err(join_error)) = flushers.join_next() => {
                    std::panic::resume_unwind(join_error.into_panic())
                }
            }
        }
    };
    if let Some(outcome) = outcome {
        shared.record_swap(outcome);
    }
    // A flusher is aborted only with this task, which is then not polled
    // again, so a join error seen here is its panic.
    while let Some(joined) = flushers.join_next().await {
        if let Err(join_error) = joined {
            std::panic::resume_unwind(join_error.into_panic());
        }
    }
}

/// Swaps out a non-empty filling buffer once its first item is `max_delay`
/// old. Sleeps against the first item's exact deadline; while the buffer is
/// empty, parks until [`Shared::first_push`]; while no buffer is free,
/// parks until [`Shared::buffer_freed`] and tries again.
async fn timer<T>(shared: &Shared<T>, max_delay: NonZeroDuration) -> std::convert::Infallible {
    loop {
        match shared.swapchain.first_push_at() {
            None => shared.first_push.notified().await,
            Some(first_push_at) => {
                tokio::time::sleep_until(first_push_at + max_delay.get()).await;
                match shared.swapchain.swap_overdue(max_delay) {
                    None => {}
                    Some(swapchain::OverdueOutcome::Swapped(outcome)) => {
                        shared.record_swap(outcome)
                    }
                    Some(swapchain::OverdueOutcome::Held) => shared.buffer_freed.notified().await,
                }
            }
        }
    }
}

/// One flush loop: take a full buffer, flush it (lent), return it empty to
/// the free buffers pool. Ends when the channel is closed and drained.
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
        let outcome = shared.swapchain.recycle(Vec::from(full));
        shared.record_recycle(outcome);
    }
}

#[cfg(test)]
mod tests;
