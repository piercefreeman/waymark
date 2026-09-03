//! The API HTTP surface: composes the API routes so that they are properly
//! documented with OpenAPI.

/// The API title, shared by the document and the docs page over it.
const TITLE: &str = "Waymark API";

/// Report the errors aide meets while it documents the routes, through
/// tracing.
///
/// aide drops them unless a handler is set, and raises them while the
/// routes are built, so this runs once, on the thread that builds the
/// routes, before the first of them is. aide's own advice is not to panic
/// here: some of its errors are false positives by design.
pub fn report_generation_errors() {
    aide::generate::on_error(|error| tracing::error!(%error, "openapi generation error"));
}

/// The API router: the given routes, plus `openapi.json` and the `docs`
/// page over it.
///
/// `api_urls` are the URLs where this API will be reachable, as written in
/// the OpenAPI document. The document lists them as its servers, and every
/// path it documents is relative to a server, so a client resolves a
/// documented operation by joining one of these URLs with the path.
pub fn router(routes: aide::axum::ApiRouter, api_urls: &[&str]) -> axum::Router {
    let mut document = openapi(api_urls);

    // Finishing fills the document from the routes it was given, so the
    // document is only complete once the API router is done.
    let router = routes.finish_api(&mut document);

    // The document and docs routes describe nothing but themselves, so they
    // stay out of the document: plain `route`s, not `api_route`s.
    router
        .route(
            "/openapi.json",
            axum::routing::get(move || async move { axum::Json(document) }),
        )
        .route(
            "/docs",
            // The spec url is relative to the docs page, so that it resolves
            // against whatever mount point the page is served under, the same
            // way every other path here does.
            axum::routing::get(
                aide::swagger::Swagger::new("openapi.json")
                    .with_title(TITLE)
                    .axum_handler(),
            ),
        )
}

/// The base document every route contributes to, naming the `api_urls`
/// as its servers.
fn openapi(api_urls: &[&str]) -> aide::openapi::OpenApi {
    aide::openapi::OpenApi {
        info: aide::openapi::Info {
            title: TITLE.to_owned(),
            version: "v1".to_owned(),
            ..Default::default()
        },
        servers: api_urls
            .iter()
            .map(|url| aide::openapi::Server {
                url: (*url).to_owned(),
                ..Default::default()
            })
            .collect(),
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

    /// The API router mounted at `mount_path` the way a consumer mounts it:
    /// as an isolated service, with the test routes.
    fn mounted(mount_path: &str) -> axum::Router {
        axum::Router::new().nest_service(mount_path, router(routes(), &[mount_path]))
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
        let (status, body) = get(mounted("/api"), "/api/openapi.json").await;
        let document: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(document["info"]["title"], "Waymark API");
        assert_eq!(document["servers"], serde_json::json!([{ "url": "/api" }]));
    }

    #[tokio::test]
    async fn serves_the_docs_page() {
        let (status, body) = get(mounted("/api"), "/api/docs").await;
        let body = String::from_utf8(body).expect("utf-8 body");

        assert_eq!(status, StatusCode::OK);

        // The page has to point at the document route beside it, relatively,
        // so that it resolves under whatever mount point the page is served at.
        assert!(
            body.contains("url: 'openapi.json'"),
            "docs page does not point at the document beside it"
        );
        assert!(
            body.contains("<title>Waymark API</title>"),
            "docs page does not carry the API title"
        );
    }

    #[tokio::test]
    async fn keeps_the_docs_page_out_of_the_document() {
        let (_status, body) = get(mounted("/api"), "/api/openapi.json").await;
        let document: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert!(
            document["paths"]["/docs"].is_null(),
            "the docs page is documented: {document}"
        );
    }

    #[tokio::test]
    async fn serves_the_routes_under_the_mount_path() {
        let router = mounted("/api");

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
        let router = router(routes(), &["/"]);

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

    #[tokio::test]
    async fn names_every_api_url_as_a_server() {
        let router = axum::Router::new().nest_service(
            "/api",
            router(routes(), &["/api", "https://example.test/waymark/api"]),
        );

        let (_status, body) = get(router, "/api/openapi.json").await;
        let document: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(
            document["servers"],
            serde_json::json!([{ "url": "/api" }, { "url": "https://example.test/waymark/api" }]),
            "document does not name every api url as a server: {document}"
        );
    }
}
