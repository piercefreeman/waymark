//! The HTTP routes serving the embedded webapp.

use axum::{
    Router,
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};
use waymark_webapp_assets::Assets;

/// Serve the SPA for browser routes, including direct visits to nested pages.
/// Merge alongside the API and health routers before starting the HTTP server.
pub fn router() -> Router {
    // A method router serves only GET and HEAD fallbacks; other methods get
    // 405 Method Not Allowed instead of a successful HTML response.
    Router::new().fallback_service(get(asset))
}

async fn asset(uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path().trim_start_matches('/');
    let in_assets = path == "assets" || path.starts_with("assets/");
    let file = Assets::get(path).or_else(|| {
        // Missing assets must not receive HTML; the bundle's assets all live
        // under `assets/`. Every other path is a browser route and falls back
        // to the entry point, including direct visits to nested SPA pages.
        if in_assets {
            return None;
        }
        Assets::get("index.html")
    });
    let Some(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };

    // Vite content-hashes every file under `assets/`, so none of them changes
    // under the same name; everything else revalidates by its entity tag.
    let cache_control = if in_assets {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let entity_tag = entity_tag(&file.metadata.sha256_hash());

    if if_none_match(&headers, &entity_tag) {
        return (
            StatusCode::NOT_MODIFIED,
            [
                (header::CACHE_CONTROL, cache_control),
                (header::ETAG, entity_tag.as_str()),
            ],
        )
            .into_response();
    }

    (
        [
            (header::CONTENT_TYPE, file.metadata.mimetype()),
            (header::CACHE_CONTROL, cache_control),
            (header::ETAG, entity_tag.as_str()),
        ],
        file.data,
    )
        .into_response()
}

/// A strong entity tag from a file's SHA-256.
fn entity_tag(sha256: &[u8; 32]) -> String {
    format!("\"{}\"", hex::encode(sha256))
}

/// Whether the request's `If-None-Match` matches `entity_tag`: `*`, or a listed
/// tag equal to it under the weak comparison the header calls for (a `W/`
/// prefix is ignored).
fn if_none_match(headers: &HeaderMap, entity_tag: &str) -> bool {
    // The header may repeat, and each value may list several tags.
    let values = headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok());
    let mut listed_tags = values.flat_map(|value| value.split(',')).map(str::trim);

    listed_tags.any(|listed_tag| {
        let opaque_tag = listed_tag.trim_start_matches("W/");
        listed_tag == "*" || opaque_tag == entity_tag
    })
}

#[cfg(test)]
mod tests;
