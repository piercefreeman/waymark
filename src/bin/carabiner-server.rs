use std::net::SocketAddr;

use anyhow::Result;
use carabiner::{
    AppConfig,
    server_client::{self, ServerConfig},
};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "carabiner-server",
    about = "Expose carabiner workflow operations over HTTP and gRPC"
)]
struct Args {
    /// HTTP address to bind, e.g. 127.0.0.1:24117
    #[arg(long)]
    http_addr: Option<SocketAddr>,
    /// gRPC address to bind, defaults to HTTP port + 1 when omitted.
    #[arg(long)]
    grpc_addr: Option<SocketAddr>,
}

const DEFAULT_HTTP_PORT: u16 = 24117;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        http_addr,
        grpc_addr,
    } = Args::parse();
    tracing_subscriber::fmt::init();

    let app_config = AppConfig::load()?;
    let http_addr = http_addr
        .or(app_config.http_addr)
        .unwrap_or_else(|| SocketAddr::new([127, 0, 0, 1].into(), DEFAULT_HTTP_PORT));
    let grpc_addr = grpc_addr
        .or(app_config.grpc_addr)
        .unwrap_or_else(|| SocketAddr::new(http_addr.ip(), http_addr.port().saturating_add(1)));

    let config = ServerConfig {
        http_addr,
        grpc_addr,
        database_url: app_config.database_url.clone(),
    };

    server_client::run_servers(config).await
}
