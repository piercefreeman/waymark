use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use super::*;

/// A backend serving a fixed answer to both reads, recording what the
/// series read was asked: the node, the range and the bucket width.
#[derive(Debug)]
struct FixedBackend {
    /// Whether every read fails.
    fail: bool,

    /// The node the last series read was given.
    seen_node_id: std::sync::Mutex<Option<waymark_ids::NodeId>>,

    /// The `from` and `to` the last series read was given.
    seen_range:
        std::sync::Mutex<Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>>,

    /// The bucket width the last series read was given.
    seen_bucket: std::sync::Mutex<Option<std::time::Duration>>,
}

/// A histogram that observed nothing.
fn empty_histogram<const N: usize>() -> waymark_essential_metrics_core::BucketedHistogram<N> {
    waymark_essential_metrics_core::BucketedHistogram {
        counts: [0; N],
        sum: 0.0,
    }
}

/// One bucket of a series answer, its last action completed a second
/// before the bucket starts.
fn bucket_sample(
    node_id: waymark_ids::NodeId,
    sampled_at: chrono::DateTime<chrono::Utc>,
    actions_completed_total: u64,
) -> waymark_essential_metrics_core::NodeSample<waymark_ids::NodeId> {
    waymark_essential_metrics_core::NodeSample {
        node_id,
        sampled_at,
        worker_pool_size: 8,
        max_in_flight_actions: 4000,
        in_flight_actions: 1,
        queued_action_dispatches: 0,
        driven_vm_runtimes: 1,
        actions_completed_total,
        last_action_completed_at: Some(sampled_at - chrono::TimeDelta::seconds(1)),
        action_dequeue_seconds: empty_histogram(),
        action_handling_seconds: empty_histogram(),
        essential_metrics_dropped_total: 0,
        observability_events_dropped_total: 0,
    }
}

impl waymark_essential_metrics_query_backend::HasNodeId for FixedBackend {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_essential_metrics_query_backend::Latest for FixedBackend {
    type Error = &'static str;

    async fn latest(
        &self,
    ) -> Result<Vec<waymark_essential_metrics_core::NodeSample<waymark_ids::NodeId>>, &'static str>
    {
        if self.fail {
            return Err("backend down");
        }
        Ok(vec![
            waymark_essential_metrics_core::NodeSample {
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
                observability_events_dropped_total: 9,
            },
            bucket_sample(
                waymark_ids::NodeId::new_uuid_v4(),
                chrono::DateTime::from_timestamp_secs(1_700_000_000).unwrap(),
                12,
            ),
        ])
    }
}

impl waymark_essential_metrics_query_backend::Series for FixedBackend {
    type Error = &'static str;

    async fn series(
        &self,
        params: waymark_essential_metrics_query_backend::series::Params<waymark_ids::NodeId>,
    ) -> Result<Vec<waymark_essential_metrics_core::NodeSample<waymark_ids::NodeId>>, &'static str>
    {
        if self.fail {
            return Err("backend down");
        }

        *self.seen_node_id.lock().unwrap() = Some(params.node_id);
        *self.seen_range.lock().unwrap() = Some((params.from, params.to));
        *self.seen_bucket.lock().unwrap() = Some(params.bucket.get());

        // Two buckets, ascending, as the store answers.
        let next_bucket = params.from + chrono::TimeDelta::from_std(params.bucket.get()).unwrap();
        Ok(vec![
            bucket_sample(params.node_id, params.from, 7),
            bucket_sample(params.node_id, next_bucket, 9),
        ])
    }
}

fn backend(fail: bool) -> Arc<FixedBackend> {
    Arc::new(FixedBackend {
        fail,
        seen_node_id: std::sync::Mutex::new(None),
        seen_range: std::sync::Mutex::new(None),
        seen_bucket: std::sync::Mutex::new(None),
    })
}

const RANGE: &str = "from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z";

async fn get(backend: &Arc<FixedBackend>, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = router(Arc::clone(backend))
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"application/json"));
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    // A rejection's body is text, not JSON; it travels as a string.
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else if json {
        serde_json::from_slice(&body).expect("json body")
    } else {
        serde_json::Value::String(String::from_utf8(body.to_vec()).expect("utf-8 body"))
    };
    (status, body)
}

/// Asserts a request is refused as a 400 whose text body gives `reason`.
async fn assert_text_400(backend: &Arc<FixedBackend>, uri: &str, reason: &str) {
    let response = router(Arc::clone(backend))
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers()[axum::http::header::CONTENT_TYPE],
        "text/plain; charset=utf-8",
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = std::str::from_utf8(&body).expect("text body");
    assert!(
        body.contains(reason),
        "the 400 body {body:?} gives {reason:?}"
    );
}

#[tokio::test]
async fn latest_serves_the_samples() {
    let backend = backend(false);
    let (status, body) = get(&backend, "/essential-metrics/nodes/latest").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_array().expect("an array").len(),
        2,
        "one sample per node",
    );
    let sample = &body[0];
    assert_eq!(sample["worker_pool_size"], 8);
    assert_eq!(sample["max_in_flight_actions"], 4000);
    assert_eq!(sample["actions_completed_total"], 41);
    assert_eq!(sample["observability_events_dropped_total"], 9);
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
async fn latest_backend_failure_is_a_500() {
    let backend = backend(true);
    let (status, _) = get(&backend, "/essential-metrics/nodes/latest").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn series_serves_the_bucketed_samples() {
    let backend = backend(false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let (status, body) = get(
        &backend,
        &format!("/essential-metrics/nodes/{node_id}/series?{RANGE}&bucket_seconds=60"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let samples = body.as_array().expect("an array");
    assert_eq!(samples.len(), 2, "one sample per bucket");
    assert_eq!(samples[0]["node_id"], node_id.to_string());
    assert_eq!(samples[0]["sampled_at"], "2023-11-14T00:00:00Z");
    assert_eq!(samples[0]["actions_completed_total"], 7);
    assert_eq!(
        samples[0]["last_action_completed_at"],
        "2023-11-13T23:59:59Z"
    );
    assert_eq!(
        samples[1]["sampled_at"], "2023-11-14T00:01:00Z",
        "ascending by time, as the backend answered",
    );
    assert_eq!(samples[1]["actions_completed_total"], 9);
    assert_eq!(*backend.seen_node_id.lock().unwrap(), Some(node_id));
    assert_eq!(
        *backend.seen_range.lock().unwrap(),
        Some((
            chrono::DateTime::from_timestamp_secs(1_699_920_000).unwrap(),
            chrono::DateTime::from_timestamp_secs(1_700_006_400).unwrap(),
        )),
        "the range reaches the backend as asked",
    );
    assert_eq!(
        *backend.seen_bucket.lock().unwrap(),
        Some(std::time::Duration::from_secs(60)),
        "the bucket width reaches the backend as asked",
    );
}

#[tokio::test]
async fn series_zero_bucket_is_a_400() {
    let backend = backend(false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    assert_text_400(
        &backend,
        &format!("/essential-metrics/nodes/{node_id}/series?{RANGE}&bucket_seconds=0"),
        "at least 1",
    )
    .await;
}

#[tokio::test]
async fn series_non_integer_bucket_is_a_400() {
    let backend = backend(false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    assert_text_400(
        &backend,
        &format!("/essential-metrics/nodes/{node_id}/series?{RANGE}&bucket_seconds=abc"),
        "bucket_seconds",
    )
    .await;
}

#[tokio::test]
async fn series_without_the_range_is_a_400() {
    let backend = backend(false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    assert_text_400(
        &backend,
        &format!("/essential-metrics/nodes/{node_id}/series?bucket_seconds=60"),
        "missing field `from`",
    )
    .await;
}

#[tokio::test]
async fn series_from_before_the_stores_range_is_a_400() {
    // The earliest instant chrono reads is below what any store holds; the
    // wire refuses it before a backend sees it.
    let backend = backend(false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    assert_text_400(
        &backend,
        &format!(
            "/essential-metrics/nodes/{node_id}/series?from=-262143-01-01T00:00:00Z&to=2023-11-15T00:00:00Z&bucket_seconds=60"
        ),
        "must be within",
    )
    .await;
}

#[tokio::test]
async fn series_bad_node_id_is_a_400() {
    let backend = backend(false);
    assert_text_400(
        &backend,
        &format!("/essential-metrics/nodes/not-a-uuid/series?{RANGE}&bucket_seconds=60"),
        "Cannot parse `node_id` with value `not-a-uuid`",
    )
    .await;
}

#[tokio::test]
async fn series_backend_failure_is_a_500() {
    let backend = backend(true);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let (status, _) = get(
        &backend,
        &format!("/essential-metrics/nodes/{node_id}/series?{RANGE}&bucket_seconds=60"),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

/// The names, locations and requiredness of an operation's parameters,
/// sorted.
fn parameters(operation: &serde_json::Value) -> Vec<(&str, &str, bool)> {
    let mut parameters: Vec<(&str, &str, bool)> = operation["parameters"]
        .as_array()
        .expect("the operation documents its parameters")
        .iter()
        .map(|parameter| {
            (
                parameter["name"].as_str().expect("parameter name"),
                parameter["in"].as_str().expect("parameter location"),
                parameter["required"].as_bool().unwrap_or(false),
            )
        })
        .collect();
    parameters.sort();
    parameters
}

#[test]
fn documents_every_operation() {
    // The document must build without aide recording a generation error.
    aide::generate::on_error(|error| panic!("generation error: {error}"));

    let mut document = aide::openapi::OpenApi::default();
    let _router = router(backend(false)).finish_api(&mut document);
    let document = serde_json::to_value(&document).expect("document serializes");

    let latest = &document["paths"]["/essential-metrics/nodes/latest"]["get"];
    let series = &document["paths"]["/essential-metrics/nodes/{node_id}/series"]["get"];

    assert!(
        latest["parameters"].is_null(),
        "latest takes no parameters: {latest}"
    );
    assert_eq!(
        parameters(series),
        [
            ("bucket_seconds", "query", true),
            ("from", "query", true),
            ("node_id", "path", true),
            ("to", "query", true),
        ],
    );

    assert!(
        latest["description"]
            .as_str()
            .is_some_and(|description| description.contains("no particular node order")),
        "the description states the order: {latest}",
    );
    assert!(
        series["description"]
            .as_str()
            .is_some_and(|description| description.contains("Buckets are laid from `from`")),
        "the description states the bucketing: {series}",
    );
    for operation in [latest, series] {
        assert_eq!(
            operation["responses"]["200"]["content"]["application/json"]["schema"]["items"]["$ref"],
            "#/components/schemas/NodeSample",
        );
        assert_eq!(
            operation["responses"]["500"]["description"],
            "The samples could not be read from the store.",
        );
    }

    // Only the series read takes parameters, so only it answers a 400.
    assert_eq!(
        series["responses"]["400"]["description"],
        "A parameter could not be read; the reason, as text.",
    );
    assert!(
        series["responses"]["400"]["content"]["text/plain; charset=utf-8"].is_object(),
        "the 400 is text: {series}",
    );
}
