//! The buffer-rotation mechanism, and nothing else: buffers move between
//! the filling slot, the free pool, and the full channel. It counts
//! nothing and notifies nobody — every operation reports what happened,
//! and the caller does the accounting.
//!
//! The invariants live behind this module boundary: buffers are allocated
//! at construction only and never grow; only non-empty buffers are sent;
//! a swap always finds a channel slot; after [`Swapchain::close`], no item
//! enters a buffer unreported.

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

    /// Set by [`Swapchain::close`]; pushes are refused from then on.
    closed: bool,
}

/// See the module docs.
pub struct Swapchain<T> {
    /// The buffer currently accepting pushes.
    filling: std::sync::Mutex<Filling<T>>,

    /// Empty buffers with their capacity intact; at most `buffers − 1` deep.
    free: std::sync::Mutex<Vec<Vec<T>>>,

    /// Moves full buffers to the flushers, non-emptiness carried by the
    /// type. Capacity is `buffers`, and the filling buffer never occupies a
    /// slot, so a send after a successful free-pop cannot fail.
    full_tx: mpsc::Sender<NEVec<T>>,

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

    /// Items refused: the swapchain is closed.
    pub discarded_closed: usize,
}

/// What a swap did with the filling buffer.
pub enum SwapOutcome {
    /// The buffer went to the flushers.
    Sent,

    /// No buffer was free; this many items were discarded.
    DroppedFull(usize),
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
                    closed: false,
                }),
                free: std::sync::Mutex::new(free),
                full_tx,
                max_batch,
            },
            full_rx,
        )
    }

    /// Hand one item in.
    pub fn push(&self, item: T) -> PushOutcome {
        let mut filling = self.filling.lock().unwrap();
        let mut outcome = PushOutcome::default();
        if filling.closed {
            outcome.discarded_closed = 1;
            return outcome;
        }
        self.push_locked(&mut filling, item, &mut outcome);
        outcome
    }

    /// [`push`](Self::push) every item, under one lock.
    pub fn push_many(&self, items: impl IntoIterator<Item = T>) -> PushOutcome {
        let mut filling = self.filling.lock().unwrap();
        let mut outcome = PushOutcome::default();
        if filling.closed {
            outcome.discarded_closed = items.into_iter().count();
            return outcome;
        }
        for item in items {
            self.push_locked(&mut filling, item, &mut outcome);
        }
        outcome
    }

    /// When the filling buffer's first item arrived; `None` while empty.
    pub fn first_push_at(&self) -> Option<tokio::time::Instant> {
        self.filling.lock().unwrap().first_push_at
    }

    /// Swap out a non-empty filling buffer whose first item is at least
    /// `max_delay` old. The age check happens under the lock, so it cannot
    /// race a concurrent swap-and-refill.
    pub fn swap_overdue(&self, max_delay: NonZeroDuration) -> Option<SwapOutcome> {
        let mut filling = self.filling.lock().unwrap();
        match filling.first_push_at {
            Some(at) if at + max_delay.get() <= tokio::time::Instant::now() => {
                Some(self.swap(&mut filling))
            }
            _ => None,
        }
    }

    /// Close the intake and swap out a final non-empty filling buffer. The
    /// closed flag lives under the filling lock, so no item can enter a
    /// buffer unreported after this returns.
    pub fn close(&self) -> Option<SwapOutcome> {
        let mut filling = self.filling.lock().unwrap();
        filling.closed = true;
        if filling.buf.is_empty() {
            None
        } else {
            Some(self.swap(&mut filling))
        }
    }

    /// Take back a flushed buffer: cleared, capacity kept, onto the free
    /// pool.
    pub fn recycle(&self, mut buffer: Vec<T>) {
        buffer.clear();
        self.free.lock().unwrap().push(buffer);
    }

    /// The body of a push, under the already-held `filling` lock.
    fn push_locked(&self, filling: &mut Filling<T>, item: T, outcome: &mut PushOutcome) {
        if filling.buf.is_empty() {
            filling.first_push_at = Some(tokio::time::Instant::now());
            outcome.first_push = true;
        }
        filling.buf.push(item); // into reserved capacity
        if filling.buf.len() == self.max_batch
            && let SwapOutcome::DroppedFull(discarded) = self.swap(filling)
        {
            outcome.discarded_full += discarded;
        }
    }

    /// Exchange the filling buffer for an empty one and send the full one to
    /// the flushers; with no buffer free, discard the items. The caller
    /// holds the `filling` lock and guarantees `buf` is non-empty.
    fn swap(&self, filling: &mut Filling<T>) -> SwapOutcome {
        // Bound to an intermediate so the pool guard drops right away —
        // a match scrutinee's temporaries live for the whole match.
        let empty = { self.free.lock().unwrap().pop() };
        let outcome = match empty {
            Some(empty) => {
                let full = std::mem::replace(&mut filling.buf, empty);
                let full = NEVec::try_from_vec(full)
                    .expect("swap is never called with an empty filling buffer");
                if self.full_tx.try_send(full).is_err() {
                    unreachable!(
                        "full_tx has one slot per buffer and the filling \
                         buffer never occupies one"
                    );
                }
                SwapOutcome::Sent
            }
            None => {
                let discarded = filling.buf.len();
                filling.buf.clear(); // keeps the capacity
                SwapOutcome::DroppedFull(discarded)
            }
        };
        filling.first_push_at = None;
        outcome
    }
}

#[cfg(test)]
impl<T> Swapchain<T> {
    /// Test-only: items currently in the filling buffer.
    pub fn filling_len(&self) -> usize {
        self.filling.lock().unwrap().buf.len()
    }

    /// Test-only: the filling buffer's capacity, for the no-reallocation
    /// assertion.
    pub fn filling_capacity(&self) -> usize {
        self.filling.lock().unwrap().buf.capacity()
    }

    /// Test-only: capacity of every buffer resting in the free pool.
    pub fn free_capacities(&self) -> Vec<usize> {
        self.free
            .lock()
            .unwrap()
            .iter()
            .map(Vec::capacity)
            .collect()
    }
}
