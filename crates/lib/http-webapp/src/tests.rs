use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

#[tokio::test]
async fn serves_the_embedded_spa() {
    let app = super::router();

    for uri in [
        "/",
        "/workflows",
        "/workflows/example?tab=events",
        "/workflows/Inventory.Sync",
        "/favicon.ico",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/html");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            body.as_ref(),
            super::Assets::get("index.html").unwrap().data.as_ref(),
            "{uri}"
        );
    }

    for (method, uri, expected) in [
        (Method::GET, "/assets/missing.js", StatusCode::NOT_FOUND),
        (Method::GET, "/assets/missing", StatusCode::NOT_FOUND),
        (Method::GET, "/assets", StatusCode::NOT_FOUND),
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

#[tokio::test]
async fn serves_all_compiled_assets_without_runtime_files() {
    if waymark_webapp_assets::EMBEDS_PLACEHOLDER {
        eprintln!("skipped: the webapp was not built; the placeholder page is embedded");
        return;
    }

    let app = super::router();
    let files: Vec<_> = super::Assets::iter().collect();
    assert!(files.iter().any(|path| path.ends_with(".js")));
    assert!(files.iter().any(|path| path.ends_with(".css")));
    for path in files {
        let file = super::Assets::get(&path).unwrap();
        assert!(
            matches!(file.data, std::borrow::Cow::Borrowed(_)),
            "{path} was read from disk"
        );
        for method in [Method::GET, Method::HEAD] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(format!("/{path}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                file.metadata.mimetype()
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            if method == Method::HEAD {
                assert!(body.is_empty());
            } else {
                assert_eq!(body.as_ref(), file.data.as_ref(), "{path}");
            }
        }
    }
}

#[tokio::test]
async fn caches_hashed_assets_and_revalidates_by_entity_tag() {
    let app = super::router();
    let mut cases = vec![
        ("/".to_owned(), "no-cache"),
        ("/workflows".to_owned(), "no-cache"),
    ];
    // The placeholder page has no hashed assets.
    if !waymark_webapp_assets::EMBEDS_PLACEHOLDER {
        let script = super::Assets::iter()
            .find(|path| path.starts_with("assets/") && path.ends_with(".js"))
            .unwrap();
        cases.push((format!("/{script}"), "public, max-age=31536000, immutable"));
    }

    for (uri, cache_control) in cases {
        let response = get(&app, &uri, None).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(response.headers()[header::CACHE_CONTROL], cache_control);
        let entity_tag = response.headers()[header::ETAG]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(entity_tag.len(), 66, "{uri}: a quoted SHA-256 in hex");

        for if_none_match in [
            entity_tag.clone(),
            format!("W/{entity_tag}"),
            format!("\"other\", {entity_tag}"),
            "*".to_owned(),
        ] {
            let response = get(&app, &uri, Some(&if_none_match)).await;
            assert_eq!(response.status(), StatusCode::NOT_MODIFIED, "{uri}");
            assert_eq!(response.headers()[header::CACHE_CONTROL], cache_control);
            assert_eq!(response.headers()[header::ETAG], entity_tag.as_str());
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(body.is_empty(), "{uri}");
        }

        let response = get(&app, &uri, Some("\"other\"")).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
    }
}

async fn get(
    app: &axum::Router,
    uri: &str,
    if_none_match: Option<&str>,
) -> axum::response::Response {
    let mut request = Request::builder().uri(uri);
    if let Some(if_none_match) = if_none_match {
        request = request.header(header::IF_NONE_MATCH, if_none_match);
    }

    app.clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}
