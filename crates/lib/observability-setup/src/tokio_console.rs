//! The `tokio-console` layer.

use tracing_subscriber::{Layer, Registry};

use crate::ObservabilityOptions;

pub(crate) fn layer(
    options: &ObservabilityOptions,
) -> Option<impl Layer<Registry> + Send + Sync + 'static> {
    options.tokio_console.then(|| {
        let layer = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn::<Registry>();
        report_console_endpoint();
        layer
    })
}

/// Announce the console endpoint and probe (in the background) that its
/// server actually opened the port.
fn report_console_endpoint() {
    use std::net::{SocketAddr, TcpStream};
    use std::thread;
    use std::time::Duration;

    let bind = std::env::var("TOKIO_CONSOLE_BIND").unwrap_or_else(|_| "127.0.0.1:6669".to_string());
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
