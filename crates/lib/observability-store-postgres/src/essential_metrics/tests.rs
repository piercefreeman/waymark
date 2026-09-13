use nonempty_collections::NESlice;
use waymark_essential_metrics_core::NodeSample;
use waymark_essential_metrics_query_backend::{Latest as _, Series as _};
use waymark_essential_metrics_retention_backend::ApplyRetention as _;
use waymark_essential_metrics_sink_backend::AppendSamples as _;

use crate::test_helpers::test_store;

/// A histogram whose observations all landed in `bucket`, so the counts
/// are zero below it and `observations` from there on.
fn histogram<const N: usize>(
    bucket: usize,
    observations: u64,
    sum: f64,
) -> waymark_essential_metrics_core::BucketedHistogram<N> {
    waymark_essential_metrics_core::BucketedHistogram {
        counts: std::array::from_fn(|position| if position >= bucket { observations } else { 0 }),
        sum,
    }
}

fn sample(
    node_id: waymark_ids::NodeId,
    at_secs: i64,
    actions_completed_total: u64,
) -> NodeSample<waymark_ids::NodeId> {
    NodeSample {
        node_id,
        sampled_at: chrono::DateTime::from_timestamp_secs(at_secs).unwrap(),
        worker_pool_size: 6,
        max_in_flight_actions: 60,
        in_flight_actions: 4,
        queued_action_dispatches: 2,
        driven_vm_runtimes: 10,
        actions_completed_total,
        last_action_completed_at: Some(chrono::DateTime::from_timestamp_secs(at_secs).unwrap()),
        action_dequeue_seconds: histogram(2, 1, 0.5),
        action_handling_seconds: histogram(4, 1, 1.0),
        essential_metrics_dropped_total: 1,
    }
}

#[tokio::test]
async fn append_latest_series_and_retention_round_trip() {
    let store = test_store("observability_store_test_essential_metrics").await;
    let node_a = waymark_ids::NodeId::new_uuid_v4();
    let node_b = waymark_ids::NodeId::new_uuid_v4();

    let samples = [
        sample(node_a, 1_000, 10),
        sample(node_a, 1_030, 20),
        sample(node_a, 1_060, 30),
        sample(node_b, 1_060, 5),
    ];
    store
        .append_samples(NESlice::try_from_slice(&samples).expect("non-empty"))
        .await
        .expect("append samples");

    // Latest: one row per node, the newest one.
    let mut latest = store.latest().await.expect("read latest");
    latest.sort_by_key(|sample| sample.actions_completed_total);
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0].node_id, node_b);
    assert_eq!(latest[0].actions_completed_total, 5);
    assert_eq!(latest[1].node_id, node_a);
    assert_eq!(latest[1].actions_completed_total, 30);
    assert_eq!(latest[1].action_dequeue_seconds, histogram(2, 1, 0.5));
    assert_eq!(
        latest[1].last_action_completed_at,
        Some(chrono::DateTime::from_timestamp_secs(1_060).unwrap()),
    );
    assert_eq!(latest[1].action_handling_seconds, histogram(4, 1, 1.0));

    // Series: node A over [1000, 1120) in 60s buckets — two samples land
    // in the first bucket (avg gauges, max counters), one in the second.
    let series = store
        .series(waymark_essential_metrics_query_backend::series::Params {
            node_id: node_a,
            from: chrono::DateTime::from_timestamp_secs(1_000).unwrap(),
            to: chrono::DateTime::from_timestamp_secs(1_120).unwrap(),
            bucket: waymark_nonzero_duration::NonZeroDuration::from_secs(60).expect("non-zero"),
        })
        .await
        .expect("read series");
    assert_eq!(series.len(), 2);
    assert_eq!(
        series[0].sampled_at,
        chrono::DateTime::from_timestamp_secs(1_000).unwrap()
    );
    assert_eq!(series[0].actions_completed_total, 20, "max within bucket");
    assert_eq!(series[0].worker_pool_size, 6, "avg within bucket");
    assert_eq!(
        series[0].last_action_completed_at,
        Some(chrono::DateTime::from_timestamp_secs(1_030).unwrap()),
        "max within bucket",
    );
    // Histogram counts and sums add across the bucket rather than being
    // averaged — the property a quantile in their place would not have.
    assert_eq!(
        series[0].action_handling_seconds,
        histogram(4, 2, 2.0),
        "two samples' counts summed within the bucket",
    );
    assert_eq!(series[0].action_dequeue_seconds, histogram(2, 2, 1.0));
    assert_eq!(
        series[1].sampled_at,
        chrono::DateTime::from_timestamp_secs(1_060).unwrap()
    );
    assert_eq!(series[1].actions_completed_total, 30);
    assert_eq!(
        series[1].action_handling_seconds,
        histogram(4, 1, 1.0),
        "one sample alone in its bucket",
    );

    // Retention: everything before 1060 goes.
    let deleted = store
        .apply_retention(chrono::DateTime::from_timestamp_secs(1_060).unwrap())
        .await
        .expect("apply retention");
    assert_eq!(deleted, 2);
    let remaining = store.latest().await.expect("read latest after retention");
    assert_eq!(remaining.len(), 2, "both nodes still have their newest row");
}
