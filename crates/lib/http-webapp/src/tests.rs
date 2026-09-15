use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

#[tokio::test]
async fn serves_the_embedded_spa() {
    let app = super::router();

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
        assert_eq!(body, waymark_webapp_assets::INDEX_HTML, "{uri}");
    }

    for (method, uri, expected) in [
        (Method::GET, "/assets/missing.js", StatusCode::NOT_FOUND),
        (Method::GET, "/favicon.ico", StatusCode::NOT_FOUND),
        (Method::POST, "/workflows", StatusCode::METHOD_NOT_ALLOWED),
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
}
