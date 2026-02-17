//! Observability helpers for optional tracing instrumentation.

pub use waymark_observability_macros::obs;

#[cfg(feature = "trace")]
use std::sync::OnceLock;

#[cfg(feature = "trace")]
use tracing_chrome::FlushGuard;

#[derive(Clone, Debug, Default)]
pub struct ObservabilityOptions {
    pub console: bool,
    pub trace_path: Option<String>,
}

#[cfg(feature = "trace")]
static TRACE_GUARD: OnceLock<std::sync::Mutex<Option<FlushGuard>>> = OnceLock::new();

#[cfg(feature = "trace")]
fn store_trace_guard(guard: FlushGuard) {
    let cell = TRACE_GUARD.get_or_init(|| std::sync::Mutex::new(None));
    let mut slot = cell.lock().expect("trace guard lock poisoned");
    *slot = Some(guard);
}

#[cfg(feature = "trace")]
pub fn flush() {
    if let Some(cell) = TRACE_GUARD.get() {
        let mut slot = cell.lock().expect("trace guard lock poisoned");
        slot.take();
    }
}

#[cfg(feature = "trace")]
pub fn init(options: ObservabilityOptions) {
    use std::env;
    use std::net::{SocketAddr, TcpStream};
    use std::thread;
    use std::time::Duration;

    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let (chrome_layer, trace_guard) = if let Some(path) = options.trace_path.clone() {
        let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .file(path.clone())
            .build();
        eprintln!("tracing-chrome enabled (trace at {path})");
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    #[cfg(feature = "observability")]
    {
        let console_layer = options.console.then(|| {
            console_subscriber::ConsoleLayer::builder()
                .with_default_env()
                .spawn()
        });
        if let Err(err) = tracing_subscriber::registry()
            .with(console_layer)
            .with(chrome_layer)
            .try_init()
        {
            eprintln!("tracing init failed: {err}");
        }
    }
    #[cfg(not(feature = "observability"))]
    {
        if options.console {
            eprintln!(
                "tokio-console disabled. Rebuild with --features observability to enable it."
            );
        }
        if let Err(err) = tracing_subscriber::registry().with(chrome_layer).try_init() {
            eprintln!("tracing init failed: {err}");
        }
    }

    if let Some(guard) = trace_guard {
        store_trace_guard(guard);
    }

    if options.console {
        let bind = env::var("TOKIO_CONSOLE_BIND").unwrap_or_else(|_| "127.0.0.1:6669".to_string());
        eprintln!("tokio-console enabled (run `tokio-console` to connect to {bind})");
        let bind_addr: Option<SocketAddr> = bind.parse().ok();
        thread::spawn(move || {
            let Some(addr) = bind_addr else {
                return;
            };
            let mut attempts = 0;
            loop {
                attempts += 1;
                if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
                    eprintln!("tokio-console listening on {addr}");
                    break;
                }
                if attempts >= 10 {
                    eprintln!(
                        "tokio-console did not open {addr} (set TOKIO_CONSOLE_BIND to a free port)"
                    );
                    break;
                }
                thread::sleep(Duration::from_millis(200));
            }
        });
    }
}

#[cfg(not(feature = "trace"))]
pub fn init(_options: ObservabilityOptions) {
    eprintln!("Tracing disabled. Rebuild with --features trace or --features observability.");
}

#[cfg(not(feature = "trace"))]
pub fn flush() {}
