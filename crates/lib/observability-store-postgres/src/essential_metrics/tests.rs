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
        last_action_completed_at: Some(chrono::DateTime::from_timestamp_secs(at_secs - 1).unwrap()),
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

    // The second sample differs from the first in every scalar, by amounts
    // where the average, the maximum and the sum all disagree.
    let samples = [
        sample(node_a, 1_000, 10),
        NodeSample {
            worker_pool_size: 9,
            max_in_flight_actions: 70,
            in_flight_actions: 8,
            queued_action_dispatches: 6,
            driven_vm_runtimes: 20,
            essential_metrics_dropped_total: 3,
            ..sample(node_a, 1_030, 20)
        },
        sample(node_a, 1_060, 30),
        sample(node_b, 1_060, 5),
    ];
    store
        .append_samples(NESlice::try_from_slice(&samples).expect("non-empty"))
        .await
        .expect("append samples");

    // Latest: one row per node, the newest one, every field as written.
    let mut latest = store.latest().await.expect("read latest");
    latest.sort_by_key(|sample| sample.actions_completed_total);
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0], samples[3]);
    assert_eq!(latest[1], samples[2]);

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
    assert_eq!(
        series[0].worker_pool_size, 8,
        "avg within bucket, rounded: 6 and 9 average to 7.5",
    );
    assert_eq!(series[0].max_in_flight_actions, 65, "avg within bucket");
    assert_eq!(series[0].in_flight_actions, 6, "avg within bucket");
    assert_eq!(series[0].queued_action_dispatches, 4, "avg within bucket");
    assert_eq!(series[0].driven_vm_runtimes, 15, "avg within bucket");
    assert_eq!(
        series[0].essential_metrics_dropped_total, 3,
        "max within bucket"
    );
    assert_eq!(
        series[0].last_action_completed_at,
        Some(chrono::DateTime::from_timestamp_secs(1_029).unwrap()),
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
    // This sample is alone in its bucket, and 1060 is itself a bucket start
    // (from 1000, 60 s wide), so it comes back as written: every aggregate
    // over one row is that row's own value.
    assert_eq!(series[1], samples[2], "one sample alone in its bucket");

    // Retention: everything before 1060 goes.
    let deleted = store
        .apply_retention(chrono::DateTime::from_timestamp_secs(1_060).unwrap())
        .await
        .expect("apply retention");
    assert_eq!(deleted, 2);
    let remaining = store.latest().await.expect("read latest after retention");
    assert_eq!(remaining.len(), 2, "both nodes still have their newest row");
}

/// The latest read of a store no node has sampled into yet is empty.
#[tokio::test]
async fn latest_of_an_empty_store_is_empty() {
    let store = test_store("observability_store_test_essential_metrics_empty").await;

    let latest = store.latest().await.expect("read latest");

    assert!(latest.is_empty(), "{latest:?}");
}

/// The samples' batches commit in any order: one that arrives late never
/// moves a node's newest sample backwards.
#[tokio::test]
async fn a_late_batch_never_moves_the_latest_sample_backwards() {
    let store = test_store("observability_store_test_essential_metrics_late_batch").await;
    let node = waymark_ids::NodeId::new_uuid_v4();

    let newer = sample(node, 200, 20);
    let older = sample(node, 100, 10);
    for batch in [[newer], [older]] {
        store
            .append_samples(NESlice::try_from_slice(&batch).expect("non-empty"))
            .await
            .expect("append samples");
    }

    let latest = store.latest().await.expect("read latest");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].sampled_at.timestamp(), 200);
    assert_eq!(latest[0].actions_completed_total, 20);
}

/// The latest read walks the samples' key from its end, one node's newest
/// row per step, never scanning the samples and never walking the time
/// index.
#[tokio::test]
async fn latest_read_walks_the_samples_key() {
    let store = test_store("observability_store_test_essential_metrics_latest_plan").await;

    // Discourage a sequential scan on the empty table so the plan shows
    // the choice the query shape allows, not the size heuristic. The
    // setting is per session, so the EXPLAIN runs on the same connection.
    let mut connection = store.pool.acquire().await.expect("connection");
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *connection)
        .await
        .expect("planner setting");
    let plan: Vec<String> =
        sqlx::query_scalar(&format!("EXPLAIN {}", super::query::latest_statement()))
            .fetch_all(&mut *connection)
            .await
            .expect("explain");

    let plan = plan.join("\n");
    assert!(
        plan.contains("essential_metrics_node_samples_pkey"),
        "the read must walk the samples' key, plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan"),
        "the read must not scan, plan was:\n{plan}"
    );
    assert!(
        !plan.contains("essential_metrics_node_samples_sampled_at_idx"),
        "the read must not walk the time index, plan was:\n{plan}"
    );
}
