//! Internal keyed storage for [`State`](crate::State).
//!
//! [`Maps`] owns the entry table and the pending-eviction index, and is the
//! only place that mutates an entry's ref-count and orphan timestamp.  The
//! invariants those carry — a ref-count that only ever moves through paired
//! [`acquire`](Maps::acquire)/[`release`](Maps::release) calls, and an
//! `orphaned_since` set exactly when the last reference is released — are
//! therefore established and checked here rather than spread across the crate.

use core::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// The underlying state maps.
///
/// Entries' ref-counts and orphan timestamps are mutated only through
/// [`acquire`](Maps::acquire)/[`release`](Maps::release)/[`sweep`](Maps::sweep);
/// the fields are private to this module so nothing else can bypass them.
/// The crate's white-box tests live in a submodule here, so they retain
/// access without any of that reaching production code.
pub(crate) struct Maps<Key, Value> {
    /// A map of keys to value-entries.
    entries: DashMap<Key, Entry<Value>>,

    /// The pending evictions map.
    pending_evictions: DashMap<Key, Instant>,
}

/// A single stored value together with its liveness bookkeeping.
struct Entry<Value> {
    value: Arc<tokio::sync::OnceCell<Value>>,
    refs: usize,

    /// Timestamp of when `refs` last dropped to zero.
    ///
    /// `None` while at least one reference is held.  Set when the last
    /// reference is released and cleared by [`Maps::acquire`].  Used inside
    /// [`Maps::sweep`]'s `remove_if` to make the retention check atomic with
    /// the ref-count, eliminating a cross-map race between `pending_evictions`
    /// and `entries`.
    orphaned_since: Option<Instant>,
}

impl<Key, Value> Maps<Key, Value>
where
    Key: Eq + Hash,
{
    /// Create an empty set of maps.
    pub(crate) fn new() -> Self {
        Self {
            entries: DashMap::new(),
            pending_evictions: DashMap::new(),
        }
    }

    /// Acquire a reference to the entry for `key`, creating it if absent.
    ///
    /// Bumps the ref-count, clears any orphan timestamp, drops any pending
    /// eviction, and returns the `OnceCell` the caller should initialize.
    /// Every successful call must be paired with exactly one [`release`] once
    /// the reference is no longer needed.
    ///
    /// [`release`]: Maps::release
    pub(crate) fn acquire(&self, key: &Key) -> Arc<tokio::sync::OnceCell<Value>>
    where
        Key: Clone,
    {
        let oncecell = {
            let mut entry = self.entries.entry(key.clone()).or_insert_with(|| Entry {
                value: Arc::new(tokio::sync::OnceCell::new()),
                refs: 0,
                orphaned_since: None,
            });
            entry.refs += 1;
            entry.orphaned_since = None;
            Arc::clone(&entry.value)
            // Shard lock released here.
        };

        self.pending_evictions.remove(key);

        oncecell
    }

    /// Release one reference previously taken via [`acquire`](Maps::acquire).
    ///
    /// If this was the last reference, the entry is either removed (when the
    /// value was never produced) or marked for delayed eviction (when it
    /// holds a value the [`sweep`](Maps::sweep) should retain).
    pub(crate) fn release(&self, key: Key) {
        let now = Instant::now();

        let dashmap::Entry::Occupied(mut occupied) = self.entries.entry(key) else {
            // The entry is gone already; nothing to release.
            return;
        };

        let entry = occupied.get_mut();

        let Some(new_refs) = entry.refs.checked_sub(1) else {
            unreachable!("refs underflow"); // should be impossible
        };
        entry.refs = new_refs;

        if new_refs != 0 {
            // Other references remain — nothing to do.
            return;
        }

        if entry.value.get().is_none() {
            // Last reference and the value was never produced — remove the
            // entry so it leaves nothing behind.  Atomic with the decrement
            // above (same shard lock), so a concurrent `acquire` either sees
            // the entry with our reference still counted or not at all.
            occupied.remove();
            return;
        }

        // Last reference and the value exists — mark for delayed eviction so
        // the sweeper retains it.  Stamp both maps with the same instant so
        // the sweep's two gates agree on when the entry became orphaned.
        entry.orphaned_since = Some(now);

        // Consumes the entry and releases the shard lock.  A concurrent
        // `acquire` may revive the entry before the insert below lands,
        // leaving a stale pending eviction behind — harmless, as the sweep
        // re-checks `refs` and `orphaned_since` against `entries` before
        // evicting anything.
        let key = occupied.into_key();

        // If there was another entry in the pending evictions map due
        // to some sort of a race - the last one wins, so overwrite the
        // preexisting one to delay the eviction further.
        let previous = self.pending_evictions.insert(key, now);

        if previous.is_some() {
            // Found a race! Nothing special to do really, but we'll
            // log it just in case - if users see a lot of these it
            // would warrant an investigation, as we should be able to
            // make this even safer and more reliable. Or it could be
            // caused by a newly introduced bug.
            tracing::warn!(
                "detected race at state handle drop; \
                this is probably safe as it anticipated in the design - \
                but still logged cause it should be rare and notable \
                occurrence"
            );
        }
    }

    /// Evict every entry orphaned for longer than `retention`, passing each
    /// evicted key and value to `on_eviction`.
    pub(crate) fn sweep(
        &self,
        retention: Duration,
        mut on_eviction: impl FnMut(Key, Arc<tokio::sync::OnceCell<Value>>),
    ) where
        Key: Clone,
    {
        let now = Instant::now();

        // Empty until something has actually expired — the common sweep
        // outcome is no evictions, which this way costs no allocation.
        let mut cleanup_queue = Vec::new();
        self.pending_evictions.retain(|key, orphaned_since| {
            let retain = now.duration_since(*orphaned_since) <= retention;
            if !retain {
                cleanup_queue.push((*key).clone());
            }
            retain
        });

        for key in cleanup_queue {
            let Some((key, entry)) = self.entries.remove_if(&key, |_, entry| {
                entry.refs == 0
                    && entry
                        .orphaned_since
                        .is_some_and(|t| now.duration_since(t) > retention)
            }) else {
                // The entry either has non-zero refs (still held), has been
                // re-acquired and re-dropped with a fresh `orphaned_since`
                // (retention not yet elapsed), or was already evicted by a
                // prior sweep.  In all cases it is harmless to skip.
                //
                // The `orphaned_since` gate inside `remove_if` closes the race
                // where a handle is acquired and dropped mid-sweep: `retain`
                // strips the stale key from `pending_evictions`, but
                // `remove_if` sees the fresh `orphaned_since` and refuses to
                // evict until the full retention has elapsed.
                continue;
            };

            on_eviction(key, entry.value);
        }
    }
}

#[cfg(test)]
mod tests;
