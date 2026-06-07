use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use nonempty_collections::{IntoIteratorExt as _, NEVec};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::{
    KeepaliveInstancePinnings, Pinning, PinningStatus, PollUnpinnedInstances, ReleasePinnings,
};

use crate::maintenance::refresh_active_pinnings;
use crate::poll::poll_and_pin;

/// A stub backend for testing the VM workload manager.
#[derive(Clone)]
struct StubBackend {
    /// Pre-loaded poll results, popped in FIFO order.
    poll_results: PollResults,

    /// Record of refresh calls.
    refresh_calls: RefreshCalls,

    /// Record of release calls.
    release_calls: ReleaseCalls,
}

type PollResults = Arc<Mutex<VecDeque<Result<NEVec<u64>, StubError>>>>;
type RefreshCalls = Arc<Mutex<Vec<(Pinning<u64, chrono::DateTime<Utc>>, Vec<u64>)>>>;
type ReleaseCalls = Arc<Mutex<Vec<(u64, Vec<u64>)>>>;

#[derive(Debug, Clone)]
enum StubError {
    Internal,
}

impl waymark_workload_pinning_backend::poll::Error for StubError {
    fn kind(&self) -> waymark_workload_pinning_backend::poll::ErrorKind {
        match self {
            StubError::Internal => waymark_workload_pinning_backend::poll::ErrorKind::Internal,
        }
    }
}

impl waymark_workload_pinning_backend::HasNodeId for StubBackend {
    type NodeId = u64;
}

impl waymark_workload_pinning_backend::HasInstanceId for StubBackend {
    type InstanceId = u64;
}

impl waymark_workload_pinning_backend::HasTimestamp for StubBackend {
    type Timestamp = chrono::DateTime<Utc>;
}

impl PollUnpinnedInstances for StubBackend {
    type Error = StubError;

    async fn poll_unlocked(
        &self,
        _now: Self::Timestamp,
        _pinning: Pinning<Self::NodeId, Self::Timestamp>,
        max_items: NonZeroUsize,
    ) -> Result<NEVec<Self::InstanceId>, Self::Error> {
        let mut guard = self.poll_results.lock().expect("poll results poisoned");
        let Some(batch) = guard.pop_front() else {
            return Err(StubError::Internal);
        };
        match batch {
            Ok(ids) => {
                let limit = max_items.get();
                if ids.len().get() > limit {
                    let mut full: Vec<u64> = ids.into_iter().collect();
                    let rest = full.split_off(limit);
                    guard.push_front(Ok(NEVec::try_from_vec(rest).expect("non-empty")));
                    Ok(NEVec::try_from_vec(full).expect("non-empty"))
                } else {
                    Ok(ids)
                }
            }
            Err(error) => Err(error),
        }
    }
}

impl KeepaliveInstancePinnings for StubBackend {
    type Error = StubError;

    fn refresh_pinnings<'a>(
        &'a self,
        pinning: Pinning<Self::NodeId, Self::Timestamp>,
        instance_ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Self::InstanceId> + 'a,
    ) -> impl Future<
        Output = Result<
            NEVec<PinningStatus<Self::InstanceId, Pinning<Self::NodeId, Self::Timestamp>>>,
            Self::Error,
        >,
    > + Send
    + 'a {
        let ids: Vec<u64> = instance_ids.into_iter().collect();
        async move {
            self.refresh_calls
                .lock()
                .expect("refresh calls poisoned")
                .push((pinning, ids.clone()));
            let head = PinningStatus {
                instance_id: ids[0],
                pinning: None,
            };
            Ok(NEVec::new(head))
        }
    }
}

impl ReleasePinnings for StubBackend {
    type Error = StubError;

    fn release_pinnings<'a>(
        &'a self,
        node_id: Self::NodeId,
        instance_ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Self::InstanceId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        let ids: Vec<u64> = instance_ids.into_iter().collect();
        async move {
            self.release_calls
                .lock()
                .expect("release calls poisoned")
                .push((node_id, ids));
            Ok(())
        }
    }
}

/// Helper to construct a non-empty vec for test data.
mod helper {
    use nonempty_collections::NEVec;

    pub fn ne_vec<T>(items: Vec<T>) -> NEVec<T> {
        NEVec::try_from_vec(items).expect("NEVec must be non-empty")
    }
}

fn test_pinning_ttl() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_secs(30)).unwrap()
}

fn test_max_concurrent() -> NonZeroUsize {
    NonZeroUsize::new(3).unwrap()
}

fn test_node_id() -> u64 {
    42
}

#[tokio::test]
async fn manager_polls_and_pins_instances() {
    let id1 = 1u64;
    let id2 = 2u64;

    let backend = StubBackend {
        poll_results: Arc::new(Mutex::new(VecDeque::from(vec![Ok(helper::ne_vec(vec![
            id1, id2,
        ]))]))),
        refresh_calls: Arc::new(Mutex::new(Vec::new())),
        release_calls: Arc::new(Mutex::new(Vec::new())),
    };

    let mut active_ids: HashSet<u64> = HashSet::new();
    let pinning_ttl = test_pinning_ttl();

    let ids = poll_and_pin(&backend, test_node_id(), test_max_concurrent(), pinning_ttl)
        .await
        .expect("poll and pin");

    active_ids.extend(ids);

    assert_eq!(active_ids.len(), 2);
    assert!(active_ids.contains(&id1));
    assert!(active_ids.contains(&id2));
}

#[tokio::test]
async fn manager_respects_max_concurrent_vms() {
    let id1 = 1u64;
    let id2 = 2u64;
    let id3 = 3u64;

    let backend = StubBackend {
        poll_results: Arc::new(Mutex::new(VecDeque::from(vec![Ok(helper::ne_vec(vec![
            id1, id2, id3,
        ]))]))),
        refresh_calls: Arc::new(Mutex::new(Vec::new())),
        release_calls: Arc::new(Mutex::new(Vec::new())),
    };

    let mut active_ids: HashSet<u64> = HashSet::new();
    let pinning_ttl = test_pinning_ttl();
    let max_concurrent = NonZeroUsize::new(2).unwrap();

    // First poll claims up to 2.
    let ids = poll_and_pin(&backend, test_node_id(), max_concurrent, pinning_ttl)
        .await
        .expect("first poll");

    active_ids.extend(ids);

    assert_eq!(active_ids.len(), 2);

    // Second poll has no slots — caller skips.
    let available = max_concurrent.get().saturating_sub(active_ids.len());
    assert_eq!(available, 0);
}

#[tokio::test]
async fn manager_refreshes_pinnings_on_active_vms() {
    let id = 1u64;

    let backend = StubBackend {
        poll_results: Arc::new(Mutex::new(VecDeque::from(vec![Ok(helper::ne_vec(vec![
            id,
        ]))]))),
        refresh_calls: Arc::new(Mutex::new(Vec::new())),
        release_calls: Arc::new(Mutex::new(Vec::new())),
    };

    let mut active_ids: HashSet<u64> = HashSet::new();
    let pinning_ttl = test_pinning_ttl();

    let ids = poll_and_pin(&backend, test_node_id(), test_max_concurrent(), pinning_ttl)
        .await
        .expect("poll and pin");

    active_ids.extend(ids.iter().cloned());

    let ids = active_ids
        .iter()
        .cloned()
        .try_into_nonempty_iter()
        .expect("non-empty");
    refresh_active_pinnings(&backend, test_node_id(), ids, pinning_ttl)
        .await
        .expect("refresh pinnings");

    let refresh_calls = backend.refresh_calls.lock().expect("poisoned");
    assert_eq!(refresh_calls.len(), 1);
    assert_eq!(refresh_calls[0].1.len(), 1);
    assert_eq!(refresh_calls[0].1[0], id);
}
