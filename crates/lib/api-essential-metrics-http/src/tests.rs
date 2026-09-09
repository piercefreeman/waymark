use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use waymark_essential_metrics_core::NodeSample;
use waymark_essential_metrics_query_backend::{HasNodeId, Latest};

use super::*;

/// A backend serving a fixed latest-samples answer.
#[derive(Debug)]
struct FixedBackend {
    fail: bool,
}

impl HasNodeId for FixedBackend {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_essential_metrics_query_backend::Series for FixedBackend {
    type Error = &'static str;

    async fn series(
        &self,
        params: waymark_essential_metrics_query_backend::series::Params<waymark_ids::NodeId>,
    ) -> Result<Vec<NodeSample<waymark_ids::NodeId>>, &'static str> {
        if self.fail {
            return Err("backend down");
        }
        Ok(vec![NodeSample {
            node_id: params.node_id,
            sampled_at: params.from,
            worker_pool_size: 8,
            max_in_flight_actions: 4000,
            in_flight_actions: 1,
            queued_action_dispatches: 0,
            driven_vm_runtimes: 1,
            actions_completed_total: 7,
            last_action_completed_at: None,
            action_dequeue_seconds: empty_histogram(),
            action_handling_seconds: empty_histogram(),
            essential_metrics_dropped_total: 0,
        }])
    }
}

/// A histogram that observed nothing.
fn empty_histogram<const N: usize>() -> waymark_essential_metrics_core::BucketedHistogram<N> {
    waymark_essential_metrics_core::BucketedHistogram {
        counts: [0; N],
        sum: 0.0,
    }
}

impl Latest for FixedBackend {
    type Error = &'static str;

    async fn latest(&self) -> Result<Vec<NodeSample<waymark_ids::NodeId>>, &'static str> {
        if self.fail {
            return Err("backend down");
        }
        Ok(vec![NodeSample {
            node_id: waymark_ids::NodeId::new_uuid_v4(),
            sampled_at: chrono::DateTime::from_timestamp_secs(1_700_000_000).unwrap(),
            worker_pool_size: 8,
            max_in_flight_actions: 4000,
            in_flight_actions: 3,
            queued_action_dispatches: 0,
            driven_vm_runtimes: 2,
            actions_completed_total: 41,
            last_action_completed_at: Some(
                chrono::DateTime::from_timestamp_secs(1_699_999_999).unwrap(),
            ),
            // Ten observations, all in the bucket bounded by 1e-3 and
            // preceded by 3e-4, so the median interpolates to halfway
            // across it.
            action_dequeue_seconds: waymark_essential_metrics_core::BucketedHistogram {
                counts: [0, 0, 0, 0, 10, 10, 10, 10, 10, 10, 10],
                sum: 0.0065,
            },
            action_handling_seconds: empty_histogram(),
            essential_metrics_dropped_total: 0,
        }])
    }
}

#[tokio::test]
async fn latest_serves_the_samples() {
    let response = router(Arc::new(FixedBackend { fail: false }))
        .oneshot(
            Request::builder()
                .uri("/essential-metrics/nodes/latest")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    let sample = &body[0];
    assert_eq!(sample["worker_pool_size"], 8);
    assert_eq!(sample["max_in_flight_actions"], 4000);
    assert_eq!(sample["actions_completed_total"], 41);
    assert_eq!(
        sample["action_handling_seconds"]["p50"],
        serde_json::Value::Null,
        "nothing was observed, so there is no median",
    );
    let dequeue_p50 = sample["action_dequeue_seconds"]["p50"]
        .as_f64()
        .expect("a median was observed");
    assert!(
        (dequeue_p50 - 0.00065).abs() < 1e-9,
        "median interpolates halfway from 3e-4 to 1e-3, got {dequeue_p50}",
    );
    assert_eq!(
        sample["action_dequeue_seconds"]["bounds"][0], 1e-5,
        "the bounds travel with the counts",
    );
    assert_eq!(sample["action_dequeue_seconds"]["sum"], 0.0065);
    assert!(
        sample["node_id"]
            .as_str()
            .expect("node id is a string")
            .len()
            == 36,
        "node id serializes as a hyphenated uuid"
    );
    assert_eq!(sample["sampled_at"], "2023-11-14T22:13:20Z");
}

#[tokio::test]
async fn series_serves_the_bucketed_samples() {
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let uri = format!(
        "/essential-metrics/nodes/{node_id}/series?from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z&bucket_seconds=60"
    );
    let response = router(Arc::new(FixedBackend { fail: false }))
        .oneshot(
            Request::builder()
                .uri(&uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    let sample = &body[0];
    assert_eq!(sample["node_id"], node_id.to_string());
    assert_eq!(sample["sampled_at"], "2023-11-14T00:00:00Z");
    assert_eq!(sample["actions_completed_total"], 7);
}

#[tokio::test]
async fn series_zero_bucket_is_a_400() {
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let uri = format!(
        "/essential-metrics/nodes/{node_id}/series?from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z&bucket_seconds=0"
    );
    let response = router(Arc::new(FixedBackend { fail: false }))
        .oneshot(
            Request::builder()
                .uri(&uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn series_non_integer_bucket_is_a_400() {
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let uri = format!(
        "/essential-metrics/nodes/{node_id}/series?from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z&bucket_seconds=abc"
    );
    let response = router(Arc::new(FixedBackend { fail: false }))
        .oneshot(
            Request::builder()
                .uri(&uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn series_bad_node_id_is_a_400() {
    let response = router(Arc::new(FixedBackend { fail: false }))
        .oneshot(
            Request::builder()
                .uri("/essential-metrics/nodes/not-a-uuid/series?from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z&bucket_seconds=60")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn backend_failure_is_a_500() {
    let response = router(Arc::new(FixedBackend { fail: true }))
        .oneshot(
            Request::builder()
                .uri("/essential-metrics/nodes/latest")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
