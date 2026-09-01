//! The API HTTP surface: composes the API routes so that they are properly
//! documented with OpenAPI.

/// The API router, served at `mount_path`: the given routes plus
/// `openapi.json`.
///
/// The document paths stay relative to `mount_path`, and the document names
/// `mount_path` as its only server, so the two come from one place and can
/// not disagree with where the routes are served.
pub fn router(mount_path: &str, routes: aide::axum::ApiRouter) -> axum::Router {
    let mut document = openapi(mount_path);

    // Finishing fills the document from the routes it was given, so the
    // document is only complete once the API router is done.
    let router = routes.finish_api(&mut document);

    // The document route describes nothing but itself, so it stays out of the
    // document: a plain `route`, not an `api_route`.
    let router = router.route(
        "/openapi.json",
        axum::routing::get(move || {
            let document = document.clone();
            async move { axum::Json(document) }
        }),
    );

    // The mount happens on the finished router, after the document is built,
    // so that the document paths stay relative to the mount point rather
    // than being prefixed with it a second time on top of the server entry.
    match mount_path {
        // Nesting at the root is not a thing in axum; the router already is
        // at the root.
        "/" => router,
        mount_path => axum::Router::new().nest(mount_path, router),
    }
}

/// The base document every mounted route contributes to, served from
/// `mount_path`.
fn openapi(mount_path: &str) -> aide::openapi::OpenApi {
    aide::openapi::OpenApi {
        info: aide::openapi::Info {
            title: "Waymark API".to_owned(),
            version: "v1".to_owned(),
            ..Default::default()
        },
        servers: vec![aide::openapi::Server {
            url: mount_path.to_owned(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt as _;
    use tower::util::ServiceExt as _;

    use super::*;

    /// A request to `uri` against `router`, with the status and the body
    /// collected.
    async fn get(router: axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
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

        (status, body.to_vec())
    }

    /// The routes every test mounts: one documented `thing`.
    fn routes() -> aide::axum::ApiRouter {
        async fn thing() -> String {
            "thing".to_owned()
        }

        aide::axum::ApiRouter::new().api_route("/thing", aide::axum::routing::get(thing))
    }

    #[tokio::test]
    async fn serves_the_openapi_document() {
        let (status, body) = get(router("/api", routes()), "/api/openapi.json").await;
        let document: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(document["info"]["title"], "Waymark API");
    }

    #[tokio::test]
    async fn serves_the_routes_under_the_mount_path() {
        let router = router("/api", routes());

        let (status, _body) = get(router.clone(), "/api/thing").await;
        assert_eq!(status, StatusCode::OK);

        let (status, _body) = get(router.clone(), "/thing").await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (_status, body) = get(router, "/api/openapi.json").await;
        let document: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        // The path stays relative to the mount point, and the mount point is
        // the server, so a client resolves the two into where the route is.
        assert!(
            document["paths"]["/thing"]["get"].is_object(),
            "documented route is missing from the document: {document}"
        );
        assert_eq!(
            document["servers"],
            serde_json::json!([{ "url": "/api" }]),
            "document does not name the mount point as its server: {document}"
        );
    }

    #[tokio::test]
    async fn serves_the_routes_at_the_root() {
        let router = router("/", routes());

        let (status, _body) = get(router.clone(), "/thing").await;
        assert_eq!(status, StatusCode::OK);

        let (_status, body) = get(router, "/openapi.json").await;
        let document: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert!(
            document["paths"]["/thing"]["get"].is_object(),
            "documented route is missing from the document: {document}"
        );
        assert_eq!(
            document["servers"],
            serde_json::json!([{ "url": "/" }]),
            "document does not name the root as its server: {document}"
        );
    }
}
