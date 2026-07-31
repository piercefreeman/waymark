fn main() {
    println!(
        "cargo::rustc-check-cfg=cfg(waymark_observability_chrome_trace,waymark_observability_tokio_console)"
    );
}
