//! The [`Sweeper`] that evicts stale entries from [`State`](crate::State),
//! and the loop that [`run`]s it.

use core::hash::Hash;
use std::sync::{Arc, Weak};

use waymark_nonzero_duration::NonZeroDuration;

use crate::storage::Maps;

/// The sweeper for a particular state that performs the eviction of stale
/// entries.
#[must_use = "the state is swept only while the sweeper is running"]
pub struct Sweeper<Key, Value> {
    retention: NonZeroDuration,
    maps: Weak<Maps<Key, Value>>,
    state_gone: tokio_util::sync::WaitForCancellationFutureOwned,
}

impl<Key, Value> Sweeper<Key, Value> {
    /// Create a sweeper over `maps` with the given `retention`; `state_gone`
    /// resolves when the [`State`](crate::State) is dropped.
    pub(crate) fn new(
        retention: NonZeroDuration,
        maps: Weak<Maps<Key, Value>>,
        state_gone: tokio_util::sync::WaitForCancellationFutureOwned,
    ) -> Self {
        Self {
            retention,
            maps,
            state_gone,
        }
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

/// Sweep with `sweeper` every `interval`, until its [`State`](crate::State)
/// is gone.
pub async fn run<Key, Value>(sweeper: Sweeper<Key, Value>, interval: NonZeroDuration)
where
    Key: Eq + Hash + Clone,
{
    let Sweeper {
        retention,
        maps,
        state_gone,
    } = sweeper;
    let mut state_gone = std::pin::pin!(state_gone);
    let mut ticker = tokio::time::interval(interval.get());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let Some(maps) = maps.upgrade() else { break };
                maps.sweep(retention.get(), |_, _| {});
            }
            () = &mut state_gone => break,
        }
    }
}
