use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use super::*;

async fn get(router: axum::Router, uri: &str) -> (StatusCode, String) {
    let response = router
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
    let body = String::from_utf8(body.to_vec()).expect("utf-8 body");

    (status, body)
}

#[tokio::test]
async fn metrics_renders_a_recorded_counter() {
    let (recorder, handle) = build().expect("build the recorder");
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("waymark_test_counter_total").increment(1);
    });

    let (status, body) = get(router(handle), "/metrics").await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("waymark_test_counter_total 1"), "{body}");
}

#[tokio::test]
async fn health_answers_ok() {
    let (_recorder, handle) = build().expect("build the recorder");

    let (status, body) = get(router(handle), "/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "OK");
}
