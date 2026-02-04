//! Observability helpers for optional tracing instrumentation.

pub use rappel_observability_macros::obs;

#[cfg(feature = "observability")]
pub fn init() {
    use std::env;
    use std::net::{SocketAddr, TcpStream};
    use std::thread;
    use std::time::Duration;

    console_subscriber::init();
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

#[cfg(not(feature = "observability"))]
pub fn init() {
    eprintln!("Observability disabled. Rebuild with --features observability to enable tracing.");
}
