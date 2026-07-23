//! Shared test helper functions.

use std::num::NonZeroUsize;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

pub(crate) fn test_pinning_ttl() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_secs(30)).unwrap()
}

pub(crate) fn test_poll_interval() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_millis(1)).unwrap()
}

pub(crate) fn test_max_concurrent() -> NonZeroUsize {
    NonZeroUsize::new(3).unwrap()
}

pub(crate) fn test_node_id() -> u64 {
    42
}

pub(crate) fn long_heartbeat() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_secs(3600)).unwrap()
}

pub(crate) fn short_heartbeat() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_millis(100)).unwrap()
}

pub(crate) fn test_unpin_retry_interval() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_millis(50)).unwrap()
}

pub(crate) fn test_fencing_margin() -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_millis(1)).unwrap()
}

pub(crate) fn test_now() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

pub(crate) fn test_pinning(
    node_id: u64,
    ttl_offset: i32,
) -> waymark_workload_pinning_backend::Pinning<u64, chrono::DateTime<chrono::Utc>> {
    let ttl = chrono::Duration::from_std(test_pinning_ttl().get()).unwrap();
    waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + ttl * ttl_offset,
    }
}
