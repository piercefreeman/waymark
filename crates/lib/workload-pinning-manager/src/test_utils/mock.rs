//! Mock backend and error type for tests.

use std::future::Future;
use std::num::NonZeroUsize;

use nonempty_collections::{IntoNonEmptyIterator, NEVec, NonEmptyIterator};
use waymark_workload_pinning_backend::{Pinning, PinningStatus};

#[derive(Debug, Clone, thiserror::Error)]
#[error("mock error")]
pub struct MockError;

mockall::mock! {
    pub Backend {
        pub fn poll_unpinned(
            &self,
            now: chrono::DateTime<chrono::Utc>,
            pinning: Pinning<u64, chrono::DateTime<chrono::Utc>>,
            max_items: NonZeroUsize,
        ) -> impl Future<Output = Result<Option<NEVec<u64>>, MockError>> + Send;

        pub fn refresh_pinnings(
            &self,
            now: chrono::DateTime<chrono::Utc>,
            pinning: Pinning<u64, chrono::DateTime<chrono::Utc>>,
            workload_ids: NEVec<u64>,
        ) -> impl Future<
            Output = Result<
                NEVec<PinningStatus<u64, Pinning<u64, chrono::DateTime<chrono::Utc>>>>,
                MockError,
            >,
        > + Send;

        pub fn release_pinnings(
            &self,
            node_id: u64,
            workload_ids: NEVec<u64>,
        ) -> impl Future<Output = Result<(), MockError>> + Send;
    }
}

impl waymark_workload_pinning_backend::HasTimestamp for MockBackend {
    type Timestamp = chrono::DateTime<chrono::Utc>;
}

impl waymark_workload_pinning_backend::HasNodeId for MockBackend {
    type NodeId = u64;
}

impl waymark_workload_pinning_backend::HasWorkloadId for MockBackend {
    type WorkloadId = u64;
}

impl waymark_workload_pinning_backend::PollUnpinnedWorkloads for MockBackend {
    type Error = MockError;

    async fn poll_unpinned(
        &self,
        now: Self::Timestamp,
        pinning: Pinning<Self::NodeId, Self::Timestamp>,
        max_items: NonZeroUsize,
    ) -> Result<Option<NEVec<Self::WorkloadId>>, Self::Error> {
        self.poll_unpinned(now, pinning, max_items).await
    }
}

impl waymark_workload_pinning_backend::KeepalivePinnings for MockBackend {
    type Error = MockError;

    fn refresh_pinnings<'a>(
        &'a self,
        now: chrono::DateTime<chrono::Utc>,
        pinning: Pinning<u64, chrono::DateTime<chrono::Utc>>,
        workload_ids: impl IntoNonEmptyIterator<Item = u64> + 'a,
    ) -> impl Future<
        Output = Result<
            NEVec<PinningStatus<u64, Pinning<u64, chrono::DateTime<chrono::Utc>>>>,
            Self::Error,
        >,
    > + Send
    + 'a {
        let ids: NEVec<u64> = workload_ids.into_nonempty_iter().collect();
        self.refresh_pinnings(now, pinning, ids)
    }
}

impl waymark_workload_pinning_backend::ReleasePinnings for MockBackend {
    type Error = MockError;

    fn release_pinnings<'a>(
        &'a self,
        node_id: u64,
        workload_ids: impl IntoNonEmptyIterator<Item = u64> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        let ids: NEVec<u64> = workload_ids.into_nonempty_iter().collect();
        self.release_pinnings(node_id, ids)
    }
}
