//! The compiled webapp, embedded in the server binary.

use axum::{
    Router,
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};

#[derive(rust_embed::Embed)]
#[folder = "$OUT_DIR/webapp"]
struct Assets;

/// Serve the SPA for browser routes, including direct visits to nested pages.
/// Merge alongside the API and health routers before starting the HTTP server.
pub fn router() -> Router {
    // A method router serves only GET and HEAD fallbacks; other methods get
    // 405 Method Not Allowed instead of a successful HTML response.
    Router::new().fallback_service(get(asset))
}

async fn asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let file = Assets::get(path).or_else(|| {
        // Missing assets must not receive HTML. Only browser routes fall back
        // to the entry point, including direct visits to nested SPA pages.
        if path == "assets" || path.starts_with("assets/") || path.rsplit('/').next()?.contains('.')
        {
            return None;
        }
        Assets::get("index.html")
    });
    let Some(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };

    (
        [
            (header::CONTENT_TYPE, file.metadata.mimetype()),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        file.data,
    )
        .into_response()
}

#[cfg(test)]
mod tests;
