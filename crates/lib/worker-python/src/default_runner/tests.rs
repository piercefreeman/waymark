use super::*;

#[test]
fn test_default_runner_detection() {
    // Should return uv as fallback if waymark-worker not in PATH
    let runner = detect();
    // Either waymark-worker was found, or we get uv with args
    if runner.script_args.is_empty() {
        assert!(
            runner
                .script_path
                .to_string_lossy()
                .contains("waymark-worker")
        );
    } else {
        assert_eq!(runner.script_path, PathBuf::from("uv"));
        assert_eq!(
            runner.script_args,
            vec!["run", "python", "-m", "waymark.worker"]
        );
    }
}
