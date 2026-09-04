use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use waymark_observability_events_query_backend::{HasNodeId, HasPayload, ListEvents, Tail};

use super::*;

/// A payload for the tests, with a one-variant closed set.
#[derive(Debug, serde::Serialize)]
struct TestPayload {
    note: &'static str,
}

#[derive(Debug)]
enum TestKind {
    Noted,
}

impl waymark_observability_events_core::kind::Tagged for TestKind {
    fn tag(self) -> &'static str {
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
/// asked, `next` as the cursor position, recording the `after` it saw.
#[derive(Debug)]
struct FixedBackend {
    /// How many events every page carries.
    events: usize,

    /// Whether every read fails.
    fail: bool,

    /// The `after` the last read was given.
    seen_after: std::sync::Mutex<Option<u64>>,
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

impl HasNodeId for FixedBackend {
    type NodeId = waymark_ids::NodeId;
}

impl HasPayload for FixedBackend {
    type Payload = TestPayload;
}

impl ListEvents for FixedBackend {
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
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);
        Ok(self.page())
    }
}

impl Tail for FixedBackend {
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
        *self.seen_after.lock().unwrap() = params.after.map(|cursor| cursor.0);
        Ok(self.page())
    }
}

fn backend(events: usize, fail: bool) -> Arc<FixedBackend> {
    Arc::new(FixedBackend {
        events,
        fail,
        seen_after: std::sync::Mutex::new(None),
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
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).expect("json body")
    };
    (status, body)
}

const RANGE: &str = "from=2023-11-14T00:00:00Z&to=2023-11-15T00:00:00Z";

#[tokio::test]
async fn list_serves_the_page_and_its_cursor() {
    let backend = backend(3, false);
    let (status, body) = get(&backend, &format!("/observability-events?{RANGE}&limit=10")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["events"].as_array().expect("events").len(), 3);
    assert_eq!(body["events"][0]["node_sequence"], 0);
    assert_eq!(body["events"][2]["node_sequence"], 2);
    assert_eq!(body["events"][0]["kind"], "noted");
    assert_eq!(body["events"][0]["payload"]["note"], "hello");
    assert_eq!(body["events"][0]["at"], "2023-11-14T22:13:20Z");
    assert_eq!(body["next"], "7", "the cursor travels as its text form");
    assert_eq!(*backend.seen_after.lock().unwrap(), None);
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
    assert_eq!(body["events"].as_array().expect("events").len(), 0);
    assert_eq!(body["next"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_bad_cursor_is_a_400() {
    let backend = backend(1, false);
    let (status, _) = get(
        &backend,
        &format!("/observability-events?{RANGE}&limit=10&after=nope"),
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
async fn tail_serves_the_node_stream() {
    let backend = backend(2, false);
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let (status, body) = get(
        &backend,
        &format!("/observability-events/nodes/{node_id}/tail?limit=10&after=3"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["events"].as_array().expect("events").len(), 2);
    assert_eq!(body["next"], "7");
    assert_eq!(*backend.seen_after.lock().unwrap(), Some(3));
}

#[tokio::test]
async fn tail_bad_node_id_is_a_400() {
    let backend = backend(1, false);
    let (status, _) = get(
        &backend,
        "/observability-events/nodes/not-a-uuid/tail?limit=10",
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
