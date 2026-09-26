use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use super::*;

/// A payload for the tests, with a one-variant closed set.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
struct TestPayload {
    note: &'static str,
}

#[derive(Debug)]
enum TestKind {
    Noted,
}

impl waymark_observability_events_core::kind::Tagged for TestKind {
    fn tag(&self) -> &'static str {
        match self {
            TestKind::Noted => "noted",
        }
    }
}

impl waymark_observability_events_core::kind::FromTag for TestKind {
    fn from_tag(tag: &str) -> Option<Self> {
        match tag {
            "noted" => Some(TestKind::Noted),
            _ => None,
        }
    }
}

impl waymark_observability_events_core::kind::Subset for TestKind {
    type RootKind = TestKind;

    fn subset() -> impl Iterator<Item = Self> {
        [TestKind::Noted].into_iter()
    }

    fn root_kind(&self) -> TestKind {
        match self {
            TestKind::Noted => TestKind::Noted,
        }
    }

    fn from_root_kind(root: TestKind) -> Option<Self> {
        Some(root)
    }
}

impl waymark_observability_events_core::Kinded for TestPayload {
    type Kind = TestKind;

    fn kind(&self) -> TestKind {
        TestKind::Noted
    }
}

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

/// A backend serving a fixed page: `events` events for whatever is
/// asked, `next` as the cursor position, recording what it was asked.
#[derive(Debug)]
struct FixedBackend {
    /// How many events every page carries.
    events: usize,

    /// Whether every read fails.
    fail: bool,

    /// The `from` and `to` the last list read was given.
    seen_range:
        std::sync::Mutex<Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>>,

    /// The node the last tail read was given.
    seen_node_id: std::sync::Mutex<Option<waymark_ids::NodeId>>,

    /// The VM the last timeline read was given.
    seen_vm_id: std::sync::Mutex<Option<waymark_ids::InstanceId>>,

    /// The `after` the last read was given.
    seen_after: std::sync::Mutex<Option<u64>>,

    /// The `limit` the last read was given.
    seen_limit: std::sync::Mutex<Option<usize>>,
}

impl FixedBackend {
    fn page(
        &self,
    ) -> Option<waymark_observability_events_query_backend::PageFor<Self, TestCursor>> {
        let counter = waymark_node_sequence::NodeSequenceCounter::new();
        let node_id = waymark_ids::NodeId::new_uuid_v4();

        let events = (0..self.events)
            .map(|_| waymark_observability_events_core::Event {
                node_id,
                node_sequence: counter.next(),
                at: chrono::DateTime::from_timestamp_secs(1_700_000_000).unwrap(),
                payload: TestPayload { note: "hello" },
            })
            .collect();
        let events = nonempty_collections::NEVec::try_from_vec(events)?;

        Some(waymark_observability_events_query_backend::Page {
            events,
            next: TestCursor(7),
        })
    }
}

impl waymark_observability_events_query_backend::HasNodeId for FixedBackend {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_observability_events_query_backend::HasPayload for FixedBackend {
    type Payload = TestPayload;
}

impl waymark_observability_events_query_backend::HasVmId for FixedBackend {
    type VmId = waymark_ids::InstanceId;
}

impl waymark_observability_events_query_backend::ListEvents for FixedBackend {
    type Cursor = TestCursor;

    type Error = &'static str;

    async fn list_events(
        &self,
        params: waymark_observability_events_query_backend::list_events::Params<TestCursor>,
    ) -> Result<
        Option<waymark_observability_events_query_backend::PageFor<Self, TestCursor>>,
        &'static str,
    > {
        if self.fail {
            return Err("backend down");
        }

        *self.seen_range.lock().unwrap() = Some((params.from, params.to));
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);
        *self.seen_limit.lock().unwrap() = Some(params.limit.get());

        Ok(self.page())
    }
}

impl waymark_observability_events_query_backend::Tail for FixedBackend {
    type Cursor = TestCursor;

    type Error = &'static str;

    async fn tail(
        &self,
        params: waymark_observability_events_query_backend::tail::Params<
            waymark_ids::NodeId,
            TestCursor,
        >,
    ) -> Result<
        Option<waymark_observability_events_query_backend::PageFor<Self, TestCursor>>,
        &'static str,
    > {
        if self.fail {
            return Err("backend down");
        }

        *self.seen_node_id.lock().unwrap() = Some(params.node_id);
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);
        *self.seen_limit.lock().unwrap() = Some(params.limit.get());

        Ok(self.page())
    }
}

impl waymark_observability_events_query_backend::VmTimeline for FixedBackend {
    type Cursor = TestCursor;

    type Error = &'static str;

    async fn vm_timeline(
        &self,
        params: waymark_observability_events_query_backend::vm_timeline::Params<
            waymark_ids::InstanceId,
            TestCursor,
        >,
    ) -> Result<
        Option<waymark_observability_events_query_backend::PageFor<Self, TestCursor>>,
        &'static str,
    > {
        if self.fail {
            return Err("backend down");
        }

        *self.seen_vm_id.lock().unwrap() = Some(params.vm_id);
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);
        *self.seen_limit.lock().unwrap() = Some(params.limit.get());

        Ok(self.page())
    }
}

fn backend(events: usize, fail: bool) -> Arc<FixedBackend> {
    Arc::new(FixedBackend {
        events,
        fail,
        seen_range: std::sync::Mutex::new(None),
        seen_node_id: std::sync::Mutex::new(None),
        seen_vm_id: std::sync::Mutex::new(None),
        seen_after: std::sync::Mutex::new(None),
        seen_limit: std::sync::Mutex::new(None),
    })
}

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

const RANGE: &str = "from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z";

#[tokio::test]
async fn list_serves_the_page_and_its_cursor() {
    let backend = backend(3, false);
    let (status, body) = get(&backend, &format!("/observability-events?{RANGE}&limit=10")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().expect("items").len(), 3);
    assert_eq!(body["items"][0]["node_sequence"], 0);
    assert_eq!(body["items"][2]["node_sequence"], 2);
    assert_eq!(body["items"][0]["kind"], "noted");
    assert_eq!(body["items"][0]["payload"]["note"], "hello");
    assert_eq!(body["items"][0]["at"], "2023-11-14T22:13:20Z");
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
    let backend = backend(1, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-events?{RANGE}&limit=10&after=41"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(*backend.seen_after.lock().unwrap(), Some(41));
}

#[tokio::test]
async fn an_empty_read_is_an_empty_page_without_a_cursor() {
    let backend = backend(0, false);
    let (status, body) = get(&backend, &format!("/observability-events?{RANGE}&limit=10")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().expect("items").len(), 0);
    assert_eq!(body["next"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_bad_cursor_is_a_400() {
    let backend = backend(1, false);
    let (status, body) = get(
        &backend,
        &format!("/observability-events?{RANGE}&limit=10&after=nope"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // The 400 carries the reason the cursor was refused.
    let body = body.as_str().expect("a text body");
    assert!(body.contains("invalid cursor"), "{body}");
}

#[tokio::test]
async fn list_from_before_the_stores_range_is_a_400() {
    // The earliest instant chrono reads is below what any store holds; the
    // wire refuses it before a backend sees it.
    let backend = backend(1, false);
    let (status, _) = get(
        &backend,
        "/observability-events?from=-262143-01-01T00:00:00Z&to=2023-11-15T00:00:00Z&limit=10",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_zero_limit_is_a_400() {
    let backend = backend(1, false);
    let (status, _) = get(&backend, &format!("/observability-events?{RANGE}&limit=0")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_limit_above_the_cap_is_a_400() {
    let backend = backend(1, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-events?{RANGE}&limit=1000"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the cap itself is served");

    let (status, _) = get(
        &backend,
        &format!("/observability-events?{RANGE}&limit=1001"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn tail_serves_the_node_stream() {
    let backend = backend(2, false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let (status, body) = get(
        &backend,
        &format!("/observability-events/nodes/{node_id}/tail?limit=10&after=3"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().expect("items").len(), 2);
    assert_eq!(body["next"], "7");
    assert_eq!(*backend.seen_node_id.lock().unwrap(), Some(node_id));
    assert_eq!(*backend.seen_after.lock().unwrap(), Some(3));
    assert_eq!(*backend.seen_limit.lock().unwrap(), Some(10));
}

#[tokio::test]
async fn tail_bad_node_id_is_a_400() {
    let backend = backend(1, false);
    let (status, body) = get(
        &backend,
        "/observability-events/nodes/not-a-uuid/tail?limit=10",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // The 400 carries the parameter and the value it could not take.
    let body = body.as_str().expect("a text body");
    assert!(
        body.contains("Cannot parse `node_id` with value `not-a-uuid`"),
        "{body}"
    );
}

/// The names and locations of an operation's parameters, sorted.
fn parameters(operation: &serde_json::Value) -> Vec<(&str, &str)> {
    let mut parameters: Vec<(&str, &str)> = operation["parameters"]
        .as_array()
        .expect("the operation documents its parameters")
        .iter()
        .map(|parameter| {
            (
                parameter["name"].as_str().expect("parameter name"),
                parameter["in"].as_str().expect("parameter location"),
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
    let _router = router(backend(0, false)).finish_api(&mut document);
    let document = serde_json::to_value(&document).expect("document serializes");

    let list = &document["paths"]["/observability-events"]["get"];
    let tail = &document["paths"]["/observability-events/nodes/{node_id}/tail"]["get"];
    let vm_timeline = &document["paths"]["/observability-events/vms/{vm_id}/timeline"]["get"];

    assert_eq!(
        parameters(list),
        [
            ("after", "query"),
            ("from", "query"),
            ("limit", "query"),
            ("to", "query"),
        ],
    );
    assert_eq!(
        parameters(tail),
        [("after", "query"), ("limit", "query"), ("node_id", "path")],
    );
    assert_eq!(
        parameters(vm_timeline),
        [("after", "query"), ("limit", "query"), ("vm_id", "path")],
    );

    for operation in [list, tail, vm_timeline] {
        assert!(
            operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
                .is_string(),
            "the 200 is the page's component: {operation}",
        );
        assert_eq!(
            operation["responses"]["400"]["description"],
            "A parameter could not be read; the reason, as text.",
        );
        assert!(
            operation["responses"]["400"]["content"]["text/plain; charset=utf-8"].is_object(),
            "the 400 is text: {operation}",
        );
        assert_eq!(
            operation["responses"]["500"]["description"],
            "The events could not be read from the store.",
        );
    }
}

#[tokio::test]
async fn vm_timeline_serves_the_vm_events() {
    let backend = backend(2, false);
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let (status, body) = get(
        &backend,
        &format!("/observability-events/vms/{vm_id}/timeline?limit=10&after=5"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().expect("items").len(), 2);
    assert_eq!(body["next"], "7");
    assert_eq!(*backend.seen_vm_id.lock().unwrap(), Some(vm_id));
    assert_eq!(*backend.seen_after.lock().unwrap(), Some(5));
    assert_eq!(*backend.seen_limit.lock().unwrap(), Some(10));
}

#[tokio::test]
async fn vm_timeline_bad_vm_id_is_a_400() {
    let backend = backend(1, false);
    let (status, _) = get(
        &backend,
        "/observability-events/vms/not-a-uuid/timeline?limit=10",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn backend_failure_is_a_500() {
    let backend = backend(1, true);
    let (status, _) = get(&backend, &format!("/observability-events?{RANGE}&limit=10")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn tail_backend_failure_is_a_500() {
    let backend = backend(1, true);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let (status, _) = get(
        &backend,
        &format!("/observability-events/nodes/{node_id}/tail?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn vm_timeline_backend_failure_is_a_500() {
    let backend = backend(1, true);
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let (status, _) = get(
        &backend,
        &format!("/observability-events/vms/{vm_id}/timeline?limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}
