use std::path::Path;

#[tokio::test]
async fn example() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_root.join("..").join("..").join("..");

    let program = waymark_support_test_python::load_fixture_ast_old(
        &repo_root.join("python/tests/fixtures_actions/literal_return.py"),
    )
    .await
    .unwrap();

    assert_eq!(program.functions.len(), 1);
}

#[tokio::test]
async fn file_not_found() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_root.join("..").join("..").join("..");

    let error =
        waymark_support_test_python::load_fixture_ast_old(&repo_root.join("no_such_file.py"))
            .await
            .unwrap_err();

    assert!(matches!(
        error,
        waymark_support_test_python::LoadFixtureAstOldError::Reader(_)
    ));
}
