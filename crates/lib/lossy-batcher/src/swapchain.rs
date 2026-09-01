//! The buffer-rotation mechanism, and nothing else: buffers move between
//! the filling slot, the free buffers pool, and the full channel. It counts
//! nothing and notifies nobody — every operation reports what happened,
//! and the caller does the accounting.
//!
//! The invariants live behind this module boundary: buffers are allocated
//! at construction only and never grow; only non-empty buffers are sent;
//! a swap always finds a channel slot; once closed — by
//! [`Swapchain::close`], or by a swap that found the receiver gone — no
//! item enters a buffer unreported, and a `push_many` whose swap closes
//! the intake refuses the rest of its items.

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_nonzero_duration::NonZeroDuration;

/// The buffer currently accepting pushes, plus the intake state that must
/// change atomically with it — one short mutex.
struct Filling<T> {
    /// Capacity is always `max_batch`; never grows.
    buf: Vec<T>,

    /// When `buf`'s first item arrived; `None` iff `buf` is empty.
    first_push_at: Option<tokio::time::Instant>,

    /// Moves full buffers to the flushers, non-emptiness carried by the
    /// type. Capacity is `buffers`, and the filling buffer never occupies a
    /// slot, so a send after a successful free-pop cannot fail.
    ///
    /// `None` once the intake is closed; pushes are refused from then on.
    /// Dropping this last sender is what lets the flush loops drain and
    /// end.
    full_tx: Option<mpsc::Sender<NEVec<T>>>,
}

/// See the module docs.
pub struct Swapchain<T> {
    /// The buffer currently accepting pushes.
    filling: std::sync::Mutex<Filling<T>>,

    /// Empty buffers with their capacity intact; at most `buffers − 1` deep.
    free: std::sync::Mutex<Vec<Vec<T>>>,

    /// Capacity of every buffer; a filling buffer reaching it is swapped.
    max_batch: usize,
}

/// What became of pushed items.
#[derive(Debug, Default)]
pub struct PushOutcome {
    /// A first item entered an empty filling buffer (the delay timer cares).
    pub first_push: bool,

    /// Items discarded: a full buffer found no free standby.
    pub discarded_full: usize,

    /// Items refused because the swapchain is closed, or discarded by a
    /// swap that found the receiver gone and closed it.
    pub discarded_closed: usize,
}

/// What a swap did with the filling buffer.
pub enum SwapOutcome {
    /// The buffer went to the flushers.
    Sent,

    /// No buffer was free; this many items were discarded.
    DroppedFull(usize),

    /// The receiver is gone — the batcher task was dropped before its
    /// `shutdown` future resolved — so the intake closed itself and this
    /// many items were discarded.
    DroppedClosed(usize),
}

/// What the delay trigger did with an overdue filling buffer.
pub enum OverdueOutcome {
    /// A buffer was free: the swap ran.
    Swapped(SwapOutcome),

    /// No buffer was free: the items stay where they are, still overdue,
    /// until [`Swapchain::recycle`] refills the free buffers pool.
    Held,
}

/// Whether the intake is still open after a push.
enum Intake {
    Open,

    /// The push's swap found the receiver gone and closed the intake.
    Closed,
}

/// What a recycle found.
pub struct RecycleOutcome {
    /// The free buffers pool was empty before this buffer returned — a
    /// held delay trigger can proceed now.
    pub free_was_empty: bool,
}

impl<T> Swapchain<T> {
    /// Allocate `buffers` buffers of `max_batch` capacity — the only buffer
    /// allocations ever made — and return the full channel the flush loops
    /// drain alongside.
    pub fn new(buffers: NonZeroUsize, max_batch: NonZeroUsize) -> (Self, mpsc::Receiver<NEVec<T>>) {
        let max_batch = max_batch.get();
        // One buffer starts as the filling one, the other `buffers - 1`
        // start free.
        let filling = Vec::with_capacity(max_batch);
        let free = (1..buffers.get())
            .map(|_| Vec::with_capacity(max_batch))
            .collect();
        let (full_tx, full_rx) = mpsc::channel::<NEVec<T>>(buffers.get());
        (
            Self {
                filling: std::sync::Mutex::new(Filling {
                    buf: filling,
                    first_push_at: None,
                    full_tx: Some(full_tx),
                }),
                free: std::sync::Mutex::new(free),
                max_batch,
            },
            full_rx,
        )
    }

    /// Hand one item in.
    pub fn push(&self, item: T) -> PushOutcome {
        let mut filling = self.filling.lock().unwrap();
        let mut outcome = PushOutcome::default();
        if filling.full_tx.is_none() {
            outcome.discarded_closed = 1;
            return outcome;
        }
        // A close under this push has nothing left to refuse: the next
        // push checks on entry.
        let _intake = self.push_locked(&mut filling, item, &mut outcome);
        outcome
    }

    /// [`push`](Self::push) every item — at least one — under one lock.
    pub fn push_many(
        &self,
        items: impl nonempty_collections::IntoNonEmptyIterator<Item = T>,
    ) -> PushOutcome {
        let mut filling = self.filling.lock().unwrap();
        let mut outcome = PushOutcome::default();
        if filling.full_tx.is_none() {
            // Nothing to do under the lock: count the refused items with
            // the iterator running unlocked.
            drop(filling);
            outcome.discarded_closed = items.into_iter().count();
            return outcome;
        }
        let mut items = items.into_iter();
        for item in items.by_ref() {
            if let Intake::Closed = self.push_locked(&mut filling, item, &mut outcome) {
                // The swap that closed the intake discarded its batch;
                // refuse the rest the same way, with the iterator running
                // unlocked.
                drop(filling);
                outcome.discarded_closed += items.count();
                break;
            }
        }
        outcome
    }

    /// When the filling buffer's first item arrived; `None` while empty.
    pub fn first_push_at(&self) -> Option<tokio::time::Instant> {
        self.filling.lock().unwrap().first_push_at
    }

    /// Swap out a non-empty filling buffer whose first item is at least
    /// `max_delay` old. The age check happens under the lock, so it cannot
    /// race a concurrent swap-and-refill. With no buffer free the items are
    /// held, not discarded: the buffer is not full, so nothing forces it.
    pub fn swap_overdue(&self, max_delay: NonZeroDuration) -> Option<OverdueOutcome> {
        let mut filling = self.filling.lock().unwrap();
        let due = match filling.first_push_at {
            Some(at) => at + max_delay.get() <= tokio::time::Instant::now(),
            None => false,
        };
        if !due {
            return None;
        }
        Some(match self.take_free() {
            Some(empty) => OverdueOutcome::Swapped(self.exchange(&mut filling, empty)),
            None => OverdueOutcome::Held,
        })
    }

    /// Close the intake and swap out a final non-empty filling buffer. The
    /// sender lives under the filling lock, so no item can enter a buffer
    /// unreported after this returns; dropping it lets the flush loops
    /// drain and end. Idempotent.
    pub fn close(&self) -> Option<SwapOutcome> {
        let mut filling = self.filling.lock().unwrap();
        let outcome = if filling.buf.is_empty() {
            None
        } else {
            Some(self.swap(&mut filling))
        };
        filling.full_tx = None;
        outcome
    }

    /// Take back a flushed buffer: cleared, capacity kept, onto the free
    /// pool.
    pub fn recycle(&self, mut buffer: Vec<T>) -> RecycleOutcome {
        buffer.clear();
        let mut free = self.free.lock().unwrap();
        let free_was_empty = free.is_empty();
        free.push(buffer);
        RecycleOutcome { free_was_empty }
    }

    /// The body of a push, under the already-held `filling` lock. Reports
    /// the intake closing under it, which only a swap can cause.
    fn push_locked(&self, filling: &mut Filling<T>, item: T, outcome: &mut PushOutcome) -> Intake {
        if filling.buf.is_empty() {
            filling.first_push_at = Some(tokio::time::Instant::now());
            outcome.first_push = true;
        }
        filling.buf.push(item); // into reserved capacity
        if filling.buf.len() < self.max_batch {
            return Intake::Open;
        }
        // Full: it goes out now, or the next push would grow it.
        match self.swap(filling) {
            SwapOutcome::Sent => Intake::Open,
            SwapOutcome::DroppedFull(discarded) => {
                outcome.discarded_full += discarded;
                Intake::Open
            }
            SwapOutcome::DroppedClosed(discarded) => {
                outcome.discarded_closed += discarded;
                Intake::Closed
            }
        }
    }

    /// Pop an empty buffer from the free buffers pool, holding its lock
    /// only for the pop.
    fn take_free(&self) -> Option<Vec<T>> {
        self.free.lock().unwrap().pop()
    }

    /// Exchange the filling buffer for an empty one and send the full one to
    /// the flushers; with no buffer free, discard the items. The caller
    /// holds the `filling` lock and guarantees `buf` is non-empty and the
    /// swapchain is open.
    fn swap(&self, filling: &mut Filling<T>) -> SwapOutcome {
        match self.take_free() {
            Some(empty) => self.exchange(filling, empty),
            None => {
                let discarded = filling.buf.len();
                filling.buf.clear(); // keeps the capacity
                filling.first_push_at = None;
                SwapOutcome::DroppedFull(discarded)
            }
        }
    }

    /// Install `empty` as the filling buffer and send the full one to the
    /// flushers. Finding the receiver gone closes the intake: the batch has
    /// nowhere to go, and neither will any later one. The caller holds the
    /// `filling` lock and guarantees `buf` is non-empty and the swapchain
    /// is open.
    fn exchange(&self, filling: &mut Filling<T>, empty: Vec<T>) -> SwapOutcome {
        let full = std::mem::replace(&mut filling.buf, empty);
        filling.first_push_at = None;
        let full = NEVec::try_from_vec(full)
            .expect("exchange is never called with an empty filling buffer");
        let Some(full_tx) = &filling.full_tx else {
            unreachable!("exchange is never called on a closed swapchain")
        };
        match full_tx.try_send(full) {
            Ok(()) => SwapOutcome::Sent,
            Err(mpsc::error::TrySendError::Full(_)) => unreachable!(
                "full_tx has one slot per buffer and the filling buffer never occupies one"
            ),
            Err(mpsc::error::TrySendError::Closed(full)) => {
                filling.full_tx = None;
                SwapOutcome::DroppedClosed(full.len().get())
            }
        }
    }
}

#[cfg(test)]
mod tests;
