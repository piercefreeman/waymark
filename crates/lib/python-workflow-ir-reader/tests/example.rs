use std::path::Path;

#[tokio::test]
async fn example() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_root.join("..").join("..").join("..");

    let fixtures_path = crate_root.join("tests").join("fixtures");

    let params = waymark_python_workflow_ir_reader::ReadWorkflowIrParams {
        workflow_file: &fixtures_path.join("sample.py"),
        workflow_class: None, // autodetect
        workdir: Some(&repo_root),
    };

    let program = waymark_python_workflow_ir_reader::read_workflow_ir(params)
        .await
        .unwrap();

    assert_eq!(program.functions.len(), 1);
}

#[tokio::test]
async fn not_found() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_root.join("..").join("..").join("..");

    let fixtures_path = crate_root.join("tests").join("fixtures");

    let non_existent_workflow_file = fixtures_path.join("non_existent_file.py");

    let params = waymark_python_workflow_ir_reader::ReadWorkflowIrParams {
        workflow_file: &non_existent_workflow_file,
        workflow_class: None,
        workdir: Some(&repo_root),
    };

    let error = waymark_python_workflow_ir_reader::read_workflow_ir(params)
        .await
        .unwrap_err();

    let expected_stderr_part = format!(
        r#"FileNotFoundError: [Errno 2] No such file or directory: '{}'"#,
        non_existent_workflow_file.display()
    );

    assert!(
        matches!(
            error,
            waymark_python_workflow_ir_reader::ReadWorkflowIrError::ProcessExitFail { ref output }
            if output.stdout == b"" && String::from_utf8_lossy(&output.stderr).contains(&expected_stderr_part)
        ),
        "error mismatch: {error:?}"
    );
}
