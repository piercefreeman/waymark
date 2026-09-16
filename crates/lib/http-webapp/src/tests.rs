use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

#[tokio::test]
async fn serves_the_embedded_spa_alongside_api_and_health_routes() {
    let app = axum::Router::new()
        .merge(waymark_http_healthz::router())
        .nest_service(
            "/api",
            waymark_http_api::router("/api", aide::axum::ApiRouter::new()),
        )
        .merge(super::router());

    for uri in ["/", "/workflows", "/workflows/example?tab=events"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, super::INDEX, "{uri}");
    }

    for (method, uri, expected) in [
        (Method::GET, "/api", StatusCode::NOT_FOUND),
        (Method::GET, "/api/", StatusCode::NOT_FOUND),
        (Method::GET, "/api/missing", StatusCode::NOT_FOUND),
        (Method::GET, "/assets/missing.js", StatusCode::NOT_FOUND),
        (Method::GET, "/favicon.ico", StatusCode::NOT_FOUND),
        (Method::POST, "/workflows", StatusCode::METHOD_NOT_ALLOWED),
        (Method::POST, "/api/missing", StatusCode::NOT_FOUND),
        (Method::HEAD, "/workflows", StatusCode::OK),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{uri}");
        assert!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty(),
            "{uri}"
        );
    }

    for uri in ["/healthz", "/api/openapi.json"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if uri == "/healthz" {
            assert_eq!(document["status"], "ok");
        } else {
            assert_eq!(document["info"]["title"], "Waymark API");
        }
    }
}
