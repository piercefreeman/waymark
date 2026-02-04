//! Observability helpers for optional tracing instrumentation.

pub use rappel_observability_macros::obs;

#[cfg(feature = "observability")]
pub fn init() {
    use std::env;
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let console_layer = console_subscriber::spawn();
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .try_init();
    let bind = env::var("TOKIO_CONSOLE_BIND").unwrap_or_else(|_| "127.0.0.1:6669".to_string());
    eprintln!("tokio-console enabled (run `tokio-console` to connect to {bind})");
}

#[cfg(not(feature = "observability"))]
pub fn init() {
    eprintln!("Observability disabled. Rebuild with --features observability to enable tracing.");
}
