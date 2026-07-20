//! The workloads the maintenance loop vouches for.
//!
//! See [`ActiveSet`].

use std::collections::{HashMap, HashSet};

use nonempty_collections::IntoIteratorExt as _;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// The tracked liveness of one workload's pinning.
struct PinningLiveness {
    /// The local monotonic deadline by which a refresh must confirm the
    /// pinning, or it lapses.
    lapses_at: tokio::time::Instant,

    /// Cancelled when the pinning lapses or is lost — the fence: the
    /// matching [`PinnedHandle`](crate::PinnedHandle) holds a clone.
    fence: CancellationToken,
}

/// The workloads whose pinnings the maintenance loop vouches for.
///
/// Tracking is what keeps a pinning alive: a tracked workload is
/// refreshed on every heartbeat and its liveness watched. Ceasing to
/// track a workload therefore **always** fences it — from that moment
/// its pinning is no longer refreshed and will lapse, so whoever still
/// holds the matching [`PinnedHandle`](crate::PinnedHandle) must be told
/// to stop. Both ways to stop tracking —
/// [`fence_and_stop_tracking`](ActiveSet::fence_and_stop_tracking) and
/// [`fence_all_and_into_ids`](ActiveSet::fence_all_and_into_ids) — fence
/// by construction, so a caller cannot drop a workload while leaving its
/// holder unaware.
///
/// The converse does not hold: fencing alone does not stop tracking. A
/// workload fenced on loss or lapse stays tracked — no longer refreshed,
/// deliberately left to lapse — until its eviction flows back through
/// the normal channel.
pub struct ActiveSet<Id> {
    tracked: HashMap<Id, PinningLiveness>,
}

impl<Id> ActiveSet<Id> {
    /// Create an empty set.
    pub fn new() -> Self {
        Self {
            tracked: HashMap::new(),
        }
    }

    /// How many workloads are tracked, fenced or not.
    pub fn tracked_count(&self) -> usize {
        self.tracked.len()
    }
}

impl<Id> ActiveSet<Id>
where
    Id: std::hash::Hash + Eq,
{
    /// Begin tracking a newly pinned workload.
    pub fn track_newly_pinned(
        &mut self,
        id: Id,
        lapses_at: tokio::time::Instant,
        fence: CancellationToken,
    ) {
        self.tracked
            .insert(id, PinningLiveness { lapses_at, fence });
    }

    /// Push the lapse deadline out after a refresh confirmed the pinning.
    ///
    /// A workload that is no longer tracked — evicted while the refresh
    /// was in flight — is ignored.
    pub fn extend_after_confirmed_refresh(&mut self, id: &Id, lapses_at: tokio::time::Instant) {
        let Some(liveness) = self.tracked.get_mut(id) else {
            return;
        };
        liveness.lapses_at = lapses_at;
    }
}

impl<Id> ActiveSet<Id>
where
    Id: std::hash::Hash + Eq + std::fmt::Debug,
{
    /// Fence a workload whose pinning a refresh reported held by another
    /// node, keeping it tracked so it is no longer refreshed.
    pub fn fence_lost_pinning(&mut self, id: &Id) {
        let Some(liveness) = self.tracked.get(id) else {
            return;
        };
        if liveness.fence.is_cancelled() {
            return;
        }
        warn!(?id, "pinning lost to another node; fencing workload");
        liveness.fence.cancel();
    }
}

impl<Id> ActiveSet<Id>
where
    Id: std::fmt::Debug,
{
    /// Fence every tracked workload whose lapse deadline has passed,
    /// keeping them tracked so they are no longer refreshed.
    pub fn fence_lapsed_pinnings(&mut self, now: tokio::time::Instant) {
        for (id, liveness) in &self.tracked {
            if liveness.fence.is_cancelled() || liveness.lapses_at > now {
                continue;
            }
            warn!(
                ?id,
                "pinning lapsed without a confirmed refresh; fencing workload"
            );
            liveness.fence.cancel();
        }
    }
}

impl<Id> ActiveSet<Id>
where
    Id: std::hash::Hash + Eq,
{
    /// Stop tracking an evicted workload, fencing it on the way out.
    ///
    /// The workload is no longer refreshed from this point, so its
    /// pinning would lapse regardless — the fence says so outright.
    pub fn fence_and_stop_tracking(&mut self, id: &Id) {
        let Some(liveness) = self.tracked.remove(id) else {
            return;
        };
        liveness.fence.cancel();
    }

    /// Fence every tracked workload and surrender their ids.
    ///
    /// Used when the loop exits with an error: the caller releases these
    /// pinnings during cleanup, so every holder must be told to stop
    /// before that happens.
    pub fn fence_all_and_into_ids(self) -> HashSet<Id> {
        self.tracked
            .into_iter()
            .map(|(id, liveness)| {
                liveness.fence.cancel();
                id
            })
            .collect()
    }
}

impl<Id> ActiveSet<Id>
where
    Id: Clone,
{
    /// The ids whose pinnings still need refreshing.
    ///
    /// Fenced workloads are excluded — their pinnings are deliberately
    /// left to lapse.
    pub fn ids_needing_refresh(
        &self,
    ) -> Option<impl nonempty_collections::NonEmptyIterator<Item = Id> + Clone + '_> {
        self.tracked
            .iter()
            .filter(|(_, liveness)| !liveness.fence.is_cancelled())
            .map(|(id, _)| id.clone())
            .try_into_nonempty_iter()
    }
}

impl<Id> ActiveSet<Id> {
    /// The earliest lapse deadline still standing.
    ///
    /// Fenced workloads are excluded — their signal has already fired.
    pub fn earliest_lapse_deadline(&self) -> Option<tokio::time::Instant> {
        self.tracked
            .values()
            .filter(|liveness| !liveness.fence.is_cancelled())
            .map(|liveness| liveness.lapses_at)
            .min()
    }
}

#[cfg(test)]
mod tests;
