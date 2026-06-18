//! In-memory backend for workload pinning.

use std::future::Future;
use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use nonempty_collections::{IntoIteratorExt, IntoNonEmptyIterator, NEVec, NonEmptyIterator};
use waymark_ids::InstanceId;
use waymark_workload_pinning_backend::{Pinning, PinningStatus, poll};

// ---------------------------------------------------------------------------
// HasTimestamp / HasNodeId / HasInstanceId
// ---------------------------------------------------------------------------

impl waymark_workload_pinning_backend::HasTimestamp for crate::MemoryBackend {
    type Timestamp = DateTime<Utc>;
}

impl waymark_workload_pinning_backend::HasNodeId for crate::MemoryBackend {
    type NodeId = uuid::Uuid;
}

impl waymark_workload_pinning_backend::HasInstanceId for crate::MemoryBackend {
    type InstanceId = InstanceId;
}

// ---------------------------------------------------------------------------
// PollUnpinnedInstances
// ---------------------------------------------------------------------------

impl poll::PollUnpinnedInstances for crate::MemoryBackend {
    type Error = std::convert::Infallible;

    async fn poll_unlocked(
        &self,
        now: Self::Timestamp,
        pinning: Pinning<Self::NodeId, Self::Timestamp>,
        max_items: NonZeroUsize,
    ) -> Result<Option<NEVec<Self::InstanceId>>, Self::Error> {
        let mut guard = self.workload_pinnings.lock().unwrap();

        let mut claimed = Vec::new();
        for (instance_id, current) in guard.iter_mut() {
            if claimed.len() >= max_items.get() {
                break;
            }
            let is_unpinned = match current {
                None => true,
                Some(entry) => entry.expires_at <= now,
            };
            if is_unpinned {
                *current = Some(crate::PinningEntry {
                    node_id: pinning.node_id,
                    expires_at: pinning.expires_at,
                });
                claimed.push(*instance_id);
            }
        }

        let Some(claimed) = claimed.try_into_nonempty_iter() else {
            return Ok(None);
        };

        Ok(Some(claimed.collect()))
    }
}

// ---------------------------------------------------------------------------
// KeepaliveInstancePinnings
// ---------------------------------------------------------------------------

impl waymark_workload_pinning_backend::KeepaliveInstancePinnings for crate::MemoryBackend {
    type Error = std::convert::Infallible;

    #[allow(clippy::manual_async_fn)]
    fn refresh_pinnings<'a>(
        &'a self,
        _now: Self::Timestamp,
        pinning: Pinning<Self::NodeId, Self::Timestamp>,
        instance_ids: impl IntoNonEmptyIterator<Item = Self::InstanceId> + 'a,
    ) -> impl Future<
        Output = Result<
            NEVec<PinningStatus<Self::InstanceId, Pinning<Self::NodeId, Self::Timestamp>>>,
            Self::Error,
        >,
    > + Send
    + 'a {
        let instance_ids: Vec<InstanceId> = instance_ids.into_iter().collect();
        async move {
            let mut guard = self.workload_pinnings.lock().unwrap();

            let mut statuses = Vec::new();
            for instance_id in &instance_ids {
                let current = guard.get_mut(instance_id);
                let status = match current {
                    Some(Some(entry)) if entry.node_id == pinning.node_id => {
                        entry.expires_at = pinning.expires_at;
                        PinningStatus {
                            instance_id: *instance_id,
                            pinning: Some(Pinning {
                                node_id: entry.node_id,
                                expires_at: entry.expires_at,
                            }),
                        }
                    }
                    Some(other) => PinningStatus {
                        instance_id: *instance_id,
                        pinning: other.as_ref().map(|entry| Pinning {
                            node_id: entry.node_id,
                            expires_at: entry.expires_at,
                        }),
                    },
                    None => PinningStatus {
                        instance_id: *instance_id,
                        pinning: None,
                    },
                };
                statuses.push(status);
            }

            let Some(statuses) = statuses.try_into_nonempty_iter() else {
                unreachable!("IntoNonEmptyIterator guarantees at least one element");
            };

            Ok(statuses.collect())
        }
    }
}

// ---------------------------------------------------------------------------
// ReleasePinnings
// ---------------------------------------------------------------------------

impl waymark_workload_pinning_backend::ReleasePinnings for crate::MemoryBackend {
    type Error = std::convert::Infallible;

    #[allow(clippy::manual_async_fn)]
    fn release_pinnings<'a>(
        &'a self,
        node_id: Self::NodeId,
        instance_ids: impl IntoNonEmptyIterator<Item = Self::InstanceId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        let instance_ids: Vec<InstanceId> = instance_ids.into_iter().collect();
        async move {
            let mut snapshots = self.vm_runtime_snapshots.lock().unwrap();
            let mut guard = self.workload_pinnings.lock().unwrap();
            for instance_id in &instance_ids {
                if let Some(Some(entry)) = guard.get(instance_id)
                    && entry.node_id == node_id
                {
                    guard.remove(instance_id);
                    snapshots.remove(instance_id);
                }
            }
            Ok(())
        }
    }
}
