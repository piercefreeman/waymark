use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use waymark_essential_metrics_core::NodeSample;
use waymark_nonzero_duration::NonZeroDuration;

use super::*;

/// A flusher recording every flushed sample.
struct RecordingFlusher {
    seen: Arc<Mutex<Vec<NodeSample<u32>>>>,
}

impl waymark_lossy_batcher::Flusher<NodeSample<u32>> for RecordingFlusher {
    type Error = String;

    async fn flush(
        &self,
        batch: nonempty_collections::NESlice<'_, NodeSample<u32>>,
    ) -> Result<(), String> {
        self.seen.lock().unwrap().extend(batch.iter().copied());
        Ok(())
    }
}

fn test_batcher(
    seen: &Arc<Mutex<Vec<NodeSample<u32>>>>,
) -> (
    waymark_lossy_batcher::BatcherHandle<NodeSample<u32>>,
    impl Future<Output = ()> + use<>,
) {
    waymark_lossy_batcher::lossy_batcher(
        "essential_metrics",
        waymark_lossy_batcher::Policy {
            buffers: NonZeroUsize::new(2).expect("non-zero"),
            max_batch: NonZeroUsize::new(1).expect("non-zero"),
            max_delay: NonZeroDuration::from_secs(60).expect("non-zero"),
            flushers: NonZeroUsize::new(1).expect("non-zero"),
        }
        .validate()
        .expect("policy is valid"),
        RecordingFlusher {
            seen: Arc::clone(seen),
        },
        std::future::pending(),
    )
}

#[tokio::test(start_paused = true)]
async fn bound_metrics_land_in_the_sample() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (batcher, batcher_task) = test_batcher(&seen);
    let node_id: u32 = 7;
    let (recorder, handle) = recorder::new();
    let sampler_task = run(
        handle,
        node_id,
        NonZeroDuration::from_secs(10).expect("non-zero"),
        batcher,
        std::future::pending(),
    );
    tokio::spawn(batcher_task);
    tokio::spawn(sampler_task);

    metrics::with_local_recorder(&recorder, || {
        metrics::gauge!(bindings::WORKER_POOL_SIZE).set(6.0);
        metrics::gauge!(bindings::MAX_IN_FLIGHT_ACTIONS).set(60.0);
        metrics::gauge!(bindings::QUEUED_ACTION_DISPATCHES).set(3.0);
        metrics::counter!(bindings::ACTIONS_ACQUIRED).increment(10);
        metrics::counter!(bindings::ACTIONS_RELEASED).increment(4);
        metrics::counter!(bindings::INSTANCES_REVIVED).increment(7);
        metrics::counter!(bindings::INSTANCES_EVICTED).increment(2);
        metrics::counter!(bindings::ACTIONS_COMPLETED).increment(4);
        metrics::gauge!(bindings::LAST_ACTION_COMPLETED).set(1_700_000_000.0);
        metrics::counter!(
            bindings::LOSSY_BATCHER_DROPPED,
            "batcher" => bindings::BATCHER_NAME,
            "reason" => "full",
        )
        .increment(2);
        metrics::counter!(
            bindings::LOSSY_BATCHER_DROPPED,
            "batcher" => bindings::BATCHER_NAME,
            "reason" => "flush_failed",
        )
        .increment(1);
        // Another batcher's drops must not count here.
        metrics::counter!(
            bindings::LOSSY_BATCHER_DROPPED,
            "batcher" => "observability_events",
            "reason" => "full",
        )
        .increment(100);
        // Unbound metrics get no-op handles.
        metrics::counter!("waymark_postgres_queries_total").increment(1000);
        for value in [1.0, 2.0, 3.0] {
            metrics::histogram!(bindings::ACTION_HANDLING_SECONDS).record(value);
        }
    });

    tokio::time::sleep(Duration::from_secs(11)).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }

    let samples = seen.lock().unwrap();
    let sample = samples.first().expect("one sample after one interval");
    assert_eq!(sample.node_id, node_id);
    assert_eq!(sample.worker_pool_size, 6);
    assert_eq!(sample.max_in_flight_actions, 60);
    assert_eq!(sample.in_flight_actions, 6);
    assert_eq!(sample.queued_action_dispatches, 3);
    assert_eq!(sample.driven_vm_runtimes, 5);
    assert_eq!(sample.actions_completed_total, 4);
    assert_eq!(
        sample.last_action_completed_at,
        Some(chrono::DateTime::from_timestamp_secs(1_700_000_000).unwrap()),
    );
    assert_eq!(sample.essential_metrics_dropped_total, 3);

    // Nothing was recorded for dequeues.
    assert_eq!(sample.action_dequeue_seconds.counts, [0; 11]);
    assert_eq!(sample.action_dequeue_seconds.sum, 0.0);

    // 1.0, 2.0 and 3.0 fall in the buckets bounded by 1.0, 2.5 and 5.0,
    // and the counts are cumulative from there on.
    assert_eq!(
        sample.action_handling_seconds.counts,
        [0, 0, 0, 0, 1, 2, 3, 3, 3, 3, 3, 3, 3],
    );
    assert_eq!(sample.action_handling_seconds.sum, 6.0);
}

#[tokio::test(start_paused = true)]
async fn histogram_counts_cover_only_their_own_interval() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (batcher, batcher_task) = test_batcher(&seen);
    let (recorder, handle) = recorder::new();
    let sampler_task = run(
        handle,
        7u32,
        NonZeroDuration::from_secs(10).expect("non-zero"),
        batcher,
        std::future::pending(),
    );
    tokio::spawn(batcher_task);
    tokio::spawn(sampler_task);

    // One interval's worth of fast observations, all in the bucket
    // bounded by 1.0.
    metrics::with_local_recorder(&recorder, || {
        for _ in 0..100 {
            metrics::histogram!(bindings::ACTION_HANDLING_SECONDS).record(1.0);
        }
    });
    settle(Duration::from_secs(11)).await;

    // The next interval sees only slow ones, in the bucket bounded by
    // 300.0. Were the counts cumulative across intervals, the hundred
    // above would still be sitting in the lower buckets.
    metrics::with_local_recorder(&recorder, || {
        for _ in 0..100 {
            metrics::histogram!(bindings::ACTION_HANDLING_SECONDS).record(100.0);
        }
    });
    settle(Duration::from_secs(10)).await;

    // A third interval records nothing at all.
    settle(Duration::from_secs(10)).await;

    let samples = seen.lock().unwrap();
    let [first, second, third] = samples
        .iter()
        .map(|sample| sample.action_handling_seconds)
        .collect::<Vec<_>>()[..3]
    else {
        panic!("three samples after three intervals, got {}", samples.len())
    };

    assert_eq!(
        first.counts,
        [0, 0, 0, 0, 100, 100, 100, 100, 100, 100, 100, 100, 100]
    );
    assert_eq!(first.sum, 100.0);
    assert_eq!(second.counts, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 100]);
    assert_eq!(second.sum, 10_000.0);
    assert_eq!(third.counts, [0; 13]);
    assert_eq!(third.sum, 0.0);
}

/// Let the sampler tick through `interval` and the batcher flush what it
/// pushed.
async fn settle(interval: Duration) {
    tokio::time::sleep(interval).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}
