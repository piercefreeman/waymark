use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use waymark_observability_state_query_backend::{GetInstance, ListInstances};

use super::*;

/// A position for the tests: a number, as text on the wire.
#[derive(Debug)]
struct TestCursor(u64);

impl waymark_cursor_core::EncodeCursor for TestCursor {
    fn encode(&self) -> String {
        self.0.to_string()
    }
}

impl waymark_cursor_core::DecodeCursor for TestCursor {
    type Error = std::num::ParseIntError;

    fn decode(text: &str) -> Result<Self, Self::Error> {
        let position = text.parse()?;

        Ok(Self(position))
    }
}

fn at(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_secs(1_700_000_000 + seconds).unwrap()
}

/// A VM that ran to completion and stopped: every field present.
fn finished(vm_id: waymark_ids::InstanceId) -> waymark_observability_state_core::InstanceState {
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    waymark_observability_state_core::InstanceState {
        vm_id,
        latest_run: Some(waymark_observability_state_core::Run {
            node_id,
            started_at: at(0),
            stopped: Some(waymark_observability_state_core::Stopped {
                at: at(2),
                reason: waymark_observability_events_payload::vm_driver::StopKind::Cancelled,
            }),
        }),
        outcome: Some(waymark_observability_state_core::Outcome {
            at: at(1),
            kind: waymark_observability_state_core::OutcomeKind::Complete,
        }),
        last_event: waymark_observability_state_core::LastEvent {
            at: at(2),
            node_id,
            node_sequence: waymark_node_sequence::NodeSequenceCounter::new().next(),
            run_sequence: 3,
            kind: waymark_observability_events_payload::vm_driver::Kind::VmStopped(
                waymark_observability_events_payload::vm_driver::StopKind::Cancelled,
            ),
        },
    }
}

/// A VM the events know without a run: only the last event.
fn bare(vm_id: waymark_ids::InstanceId) -> waymark_observability_state_core::InstanceState {
    waymark_observability_state_core::InstanceState {
        vm_id,
        latest_run: None,
        outcome: None,
        last_event: waymark_observability_state_core::LastEvent {
            at: at(5),
            node_id: waymark_ids::NodeId::new_uuid_v4(),
            node_sequence: waymark_node_sequence::NodeSequenceCounter::new().next(),
            run_sequence: 0,
            kind: waymark_observability_events_payload::vm_driver::Kind::SnapshotPersisted,
        },
    }
}

/// A backend serving a fixed page: `instances` instances for whatever
/// is asked — the first finished, the rest bare — `next` as the cursor
/// position, recording the `after` it saw; and one VM by id, `known` or
/// not.
#[derive(Debug)]
struct FixedBackend {
    /// How many instances every page carries.
    instances: usize,

    /// Whether the get read knows the VM asked for.
    known: bool,

    /// Whether every read fails.
    fail: bool,

    /// The `after` the last list read was given.
    seen_after: std::sync::Mutex<Option<u64>>,
}

impl ListInstances for FixedBackend {
    type Cursor = TestCursor;

    type Error = &'static str;

    async fn list_instances(
        &self,
        params: waymark_observability_state_query_backend::list_instances::Params<TestCursor>,
    ) -> Result<Option<waymark_observability_state_query_backend::PageFor<Self>>, &'static str>
    {
        if self.fail {
            return Err("backend down");
        }
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);

        let instances = (0..self.instances)
            .map(|index| {
                let vm_id = waymark_ids::InstanceId::new_uuid_v4();
                if index == 0 {
                    finished(vm_id)
                } else {
                    bare(vm_id)
                }
            })
            .collect();
        let Some(instances) = nonempty_collections::NEVec::try_from_vec(instances) else {
            return Ok(None);
        };

        Ok(Some(waymark_observability_state_query_backend::Page {
            instances,
            next: TestCursor(7),
        }))
    }
}

impl GetInstance for FixedBackend {
    type Error = &'static str;

    async fn get_instance(
        &self,
        vm_id: waymark_ids::InstanceId,
    ) -> Result<Option<waymark_observability_state_core::InstanceState>, &'static str> {
        if self.fail {
            return Err("backend down");
        }

        Ok(self.known.then(|| finished(vm_id)))
    }
}

fn backend(instances: usize, known: bool, fail: bool) -> Arc<FixedBackend> {
    Arc::new(FixedBackend {
        instances,
        known,
        fail,
        seen_after: std::sync::Mutex::new(None),
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
    // A rejection of the extractor's own (a missing query field) is
    // plain text; only a JSON body is read as one.
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
    let body = if json {
        serde_json::from_slice(&body).expect("json body")
    } else {
        serde_json::Value::Null
    };
    (status, body)
}

#[tokio::test]
async fn list_serves_the_page_and_its_cursor() {
    let backend = backend(2, true, false);
    let (status, body) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=10"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().expect("items").len(), 2);

    let finished = &body["items"][0];
    assert!(finished["vm_id"].is_string());
    assert_eq!(finished["latest_run"]["started_at"], "2023-11-14T22:13:20Z");
    assert!(finished["latest_run"]["node_id"].is_string());
    assert_eq!(
        finished["latest_run"]["stopped"]["at"],
        "2023-11-14T22:13:22Z"
    );
    assert_eq!(
        finished["latest_run"]["stopped"]["reason"],
        "vm_driver.vm_stopped.cancelled"
    );
    assert_eq!(finished["outcome"]["at"], "2023-11-14T22:13:21Z");
    assert_eq!(finished["outcome"]["kind"], "complete");
    assert_eq!(finished["last_event"]["at"], "2023-11-14T22:13:22Z");
    assert_eq!(finished["last_event"]["node_sequence"], 0);
    assert_eq!(finished["last_event"]["run_sequence"], 3);
    assert_eq!(
        finished["last_event"]["kind"],
        "vm_driver.vm_stopped.cancelled"
    );

    let bare = &body["items"][1];
    assert_eq!(bare["latest_run"], serde_json::Value::Null);
    assert_eq!(bare["outcome"], serde_json::Value::Null);
    assert_eq!(bare["last_event"]["kind"], "vm_driver.snapshot_persisted");

    assert_eq!(body["next"], "7", "the cursor travels as its text form");
    assert_eq!(*backend.seen_after.lock().unwrap(), None);
}

#[tokio::test]
async fn list_resumes_after_the_given_cursor() {
    let backend = backend(1, true, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=10&after=41"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(*backend.seen_after.lock().unwrap(), Some(41));
}

#[tokio::test]
async fn an_empty_read_is_an_empty_page_without_a_cursor() {
    let backend = backend(0, true, false);
    let (status, body) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=10"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().expect("items").len(), 0);
    assert_eq!(body["next"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_bad_cursor_is_a_400() {
    let backend = backend(1, true, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=10&after=nope"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_zero_limit_is_a_400() {
    let backend = backend(1, true, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=0"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_limit_above_the_cap_is_a_400() {
    let backend = backend(1, true, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=100"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the cap itself is served");

    let (status, _) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=101"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_without_the_range_is_a_400() {
    let backend = backend(1, true, false);
    let (status, _) = get(&backend, "/observability-state/instances?limit=10").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_backend_failure_is_a_500() {
    let backend = backend(1, true, true);
    let (status, _) = get(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn get_serves_the_instance() {
    let backend = backend(1, true, false);
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let (status, body) = get(&backend, &format!("/observability-state/instances/{vm_id}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["vm_id"], vm_id.to_string());
    assert_eq!(body["outcome"]["kind"], "complete");
    assert_eq!(
        body["latest_run"]["stopped"]["reason"],
        "vm_driver.vm_stopped.cancelled"
    );
}

#[tokio::test]
async fn get_unknown_vm_is_a_404() {
    let backend = backend(1, false, false);
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let (status, body) = get(&backend, &format!("/observability-state/instances/{vm_id}")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, serde_json::Value::Null);
}

#[tokio::test]
async fn get_bad_vm_id_is_a_400() {
    let backend = backend(1, true, false);
    let (status, _) = get(&backend, "/observability-state/instances/not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_backend_failure_is_a_500() {
    let backend = backend(1, true, true);
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let (status, _) = get(&backend, &format!("/observability-state/instances/{vm_id}")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

/// The stop kinds list the payload's stop tags, as a component.
#[test]
fn stop_kinds_list_the_payloads_tags() {
    let mut generator = schemars::generate::SchemaSettings::openapi3().into_generator();
    let stops = serde_json::to_value(
        generator.root_schema_for::<waymark_api_observability_events_http_types::KindTag<
            waymark_observability_events_payload::vm_driver::StopKind,
            StopKind,
        >>(),
    )
    .expect("json");

    let stops = stops["enum"].as_array().expect("stop tags");
    assert_eq!(stops.len(), 7, "{stops:?}");
    assert!(stops.iter().all(|tag| {
        tag.as_str()
            .expect("tag")
            .starts_with("vm_driver.vm_stopped.")
    }));
}
