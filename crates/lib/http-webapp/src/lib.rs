//! The compiled webapp, embedded in the server binary.

use axum::{
    Router,
    http::{StatusCode, Uri, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};

const INDEX: &str = include_str!(concat!(env!("OUT_DIR"), "/webapp/index.html"));

/// Serve the SPA for browser routes, including direct visits to nested pages.
/// Merge alongside the API and health routers before starting the HTTP server.
pub fn router() -> Router {
    Router::new().fallback_service(get(index))
}

async fn index(uri: Uri) -> Response {
    // There are no separate static assets in the single-file build.
    if uri
        .path()
        .rsplit('/')
        .next()
        .is_some_and(|name| name.contains('.'))
    {
        return StatusCode::NOT_FOUND.into_response();
    }

    ([(header::CACHE_CONTROL, "no-cache")], Html(INDEX)).into_response()
}

#[cfg(test)]
mod tests;
