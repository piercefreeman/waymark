use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub const SERVICE_NAME: &str = "carabiner";
pub const HEALTH_PATH: &str = "/healthz";
pub const DEFAULT_HTTP_PORT: u16 = 24117;

fn default_host() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

pub fn default_http_addr() -> SocketAddr {
    SocketAddr::new(default_host(), DEFAULT_HTTP_PORT)
}

pub fn default_grpc_addr() -> SocketAddr {
    SocketAddr::new(default_host(), DEFAULT_HTTP_PORT.saturating_add(1))
}

pub fn health_url(host: &str, port: u16) -> String {
    format!("http://{host}:{port}{HEALTH_PATH}")
}
