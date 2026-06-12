//! The [`Sweeper`] that evicts stale entries from [`State`](crate::State).

use core::hash::Hash;
use std::sync::{Arc, Weak};

use waymark_nonzero_duration::NonZeroDuration;

use crate::storage::Maps;

/// The sweeper for a particular state that performs the eviction of stale
/// entries.
pub struct Sweeper<Key, Value> {
    retention: NonZeroDuration,
    maps: Weak<Maps<Key, Value>>,
}

impl<Key, Value> Sweeper<Key, Value> {
    /// Create a sweeper over `maps` with the given `retention`.
    pub(crate) fn new(retention: NonZeroDuration, maps: Weak<Maps<Key, Value>>) -> Self {
        Self { retention, maps }
    }

    /// Returns whether this [`Sweeper`]'s associated [`State`](crate::State)
    /// still exists.
    ///
    /// Returns `false` if the [`State`](crate::State) has been dropped.
    pub fn associated_state_exists(&self) -> bool {
        self.maps.strong_count() > 0
    }
}

impl<Key, Value> Sweeper<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    /// Perform one sweep on the pending evictions map, evicting all the
    /// entries that have not been held on to via a
    /// [`Handle`](crate::Handle) for longer than the configured retention
    /// duration.
    ///
    /// Passes the evicted entries to the `on_eviction` callback.
    pub fn sweep_with_handler(
        &mut self,
        on_eviction: impl FnMut(Key, Arc<tokio::sync::OnceCell<Value>>),
    ) {
        let Some(maps) = self.maps.upgrade() else {
            return;
        };

        maps.sweep(self.retention.get(), on_eviction);
    }

    /// Like [`Sweeper::sweep_with_handler`] but discards the evicted entries.
    pub fn sweep(&mut self) {
        self.sweep_with_handler(|_, _| {});
    }
}
