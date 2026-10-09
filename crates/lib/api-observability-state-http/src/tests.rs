use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

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
        workflow_name: Some("CheckoutWorkflow".to_owned()),
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

/// A VM the instance state knows without a run: only the last event.
fn bare(vm_id: waymark_ids::InstanceId) -> waymark_observability_state_core::InstanceState {
    waymark_observability_state_core::InstanceState {
        vm_id,
        workflow_name: None,
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
/// position, recording the range, the `after` and the `limit` it saw;
/// and one VM by id, `known` or not.
#[derive(Debug)]
struct FixedBackend {
    /// How many instances every page carries.
    instances: usize,

    /// Whether the get read knows the VM asked for.
    known: bool,

    /// Whether every read fails.
    fail: bool,

    /// The `from` and `to` the last list read was given.
    seen_range:
        std::sync::Mutex<Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>>,

    /// The `after` the last list read was given.
    seen_after: std::sync::Mutex<Option<u64>>,

    /// The `limit` the last list read was given.
    seen_limit: std::sync::Mutex<Option<usize>>,
}

impl waymark_observability_state_query_backend::ListInstances for FixedBackend {
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
        *self.seen_range.lock().unwrap() = Some((params.from, params.to));
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);
        *self.seen_limit.lock().unwrap() = Some(params.limit.get());

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

impl waymark_observability_state_query_backend::GetInstance for FixedBackend {
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
        seen_range: std::sync::Mutex::new(None),
        seen_after: std::sync::Mutex::new(None),
        seen_limit: std::sync::Mutex::new(None),
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
    assert_eq!(finished["workflow_name"], "CheckoutWorkflow");
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
    assert_eq!(bare["workflow_name"], serde_json::Value::Null);
    assert_eq!(bare["latest_run"], serde_json::Value::Null);
    assert_eq!(bare["outcome"], serde_json::Value::Null);
    assert_eq!(bare["last_event"]["kind"], "vm_driver.snapshot_persisted");

    assert_eq!(body["next"], "7", "the cursor travels as its text form");
    assert_eq!(
        *backend.seen_range.lock().unwrap(),
        Some((
            chrono::DateTime::from_timestamp_secs(1_699_920_000).unwrap(),
            chrono::DateTime::from_timestamp_secs(1_700_006_400).unwrap(),
        )),
        "the range reaches the backend as asked",
    );
    assert_eq!(*backend.seen_after.lock().unwrap(), None);
    assert_eq!(*backend.seen_limit.lock().unwrap(), Some(10));
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
    assert_text_400(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=10&after=nope"),
        "invalid cursor",
    )
    .await;
}

#[tokio::test]
async fn list_zero_limit_is_a_400() {
    let backend = backend(1, true, false);
    assert_text_400(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=0"),
        "limit must be within 1..=100",
    )
    .await;
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

    assert_text_400(
        &backend,
        &format!("/observability-state/instances?{RANGE}&limit=101"),
        "limit must be within 1..=100",
    )
    .await;
}

#[tokio::test]
async fn list_without_the_range_is_a_400() {
    let backend = backend(1, true, false);
    assert_text_400(
        &backend,
        "/observability-state/instances?limit=10",
        "missing field `from`",
    )
    .await;
}

#[tokio::test]
async fn list_from_before_the_stores_range_is_a_400() {
    // The earliest instant chrono reads is below what any store holds; the
    // wire refuses it before a backend sees it.
    let backend = backend(1, true, false);
    assert_text_400(
        &backend,
        "/observability-state/instances?from=-262143-01-01T00:00:00Z&to=2023-11-15T00:00:00Z&limit=10",
        "must be within",
    )
    .await;
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
    assert_eq!(body["workflow_name"], "CheckoutWorkflow");
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
    assert_text_400(
        &backend,
        "/observability-state/instances/not-a-uuid",
        "Cannot parse `vm_id` with value `not-a-uuid`",
    )
    .await;
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
    let _router = router(backend(0, true, false)).finish_api(&mut document);
    let document = serde_json::to_value(&document).expect("document serializes");

    let list = &document["paths"]["/observability-state/instances"]["get"];
    let get = &document["paths"]["/observability-state/instances/{vm_id}"]["get"];

    assert_eq!(
        parameters(list),
        [
            ("after", "query", false),
            ("from", "query", true),
            ("limit", "query", true),
            ("to", "query", true),
        ],
    );
    assert_eq!(parameters(get), [("vm_id", "path", true)]);

    assert_eq!(
        list["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/InstancePage",
    );
    assert_eq!(
        get["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Instance",
    );
    for operation in [list, get] {
        assert!(
            operation["description"]
                .as_str()
                .is_some_and(|description| description.contains("as of the last flush")),
            "the description states the freshness: {operation}",
        );
        assert_eq!(
            operation["responses"]["400"]["description"],
            "A parameter could not be read; the reason, as text.",
        );
        assert!(
            operation["responses"]["400"]["content"]["text/plain; charset=utf-8"].is_object(),
            "the 400 is text: {operation}",
        );
    }
    assert_eq!(
        list["responses"]["500"]["description"],
        "The instances could not be read from the store.",
    );
    assert_eq!(
        get["responses"]["404"]["description"],
        "The instance state knows no VM with this id.",
    );
    assert_eq!(
        get["responses"]["500"]["description"],
        "The instance could not be read from the store.",
    );
}
