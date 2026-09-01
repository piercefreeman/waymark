//! The `/healthz` route: the load-balancer probe.

use axum::{Json, Router, routing::get};

/// Health check response.
#[derive(Debug, serde::Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

/// The routes of the health domain.
pub fn router() -> Router {
    Router::new().route("/healthz", get(healthz))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt as _;
    use tower::util::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn reports_ok() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
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
        assert_eq!(body["status"], "ok");
    }
}
