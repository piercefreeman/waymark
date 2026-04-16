pub static MAX_RUNNER_STATE_NODES: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("WAYMARK_UNSTABLE_MAX_RUNNER_STATE_NODES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
});
