fn main() {
    println!(
        "cargo::rustc-check-cfg=cfg(waymark_observability_trace,waymark_observability_tokio_console)"
    );
}
