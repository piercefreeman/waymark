//! Config for the HTTP server.

use std::net::SocketAddr;

/// Configuration for the HTTP server.
#[derive(Debug, Clone, Copy)]
pub struct HttpConfig {
    /// Whether the HTTP server is enabled.
    pub enabled: bool,

    /// The address to serve on.
    pub addr: SocketAddr,
}

/// Error returned when reading an [`HttpConfig`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The enabled flag could not be read.
    #[error(transparent)]
    Enabled(#[from] envfury::Error<envfury::OrParseError<std::str::ParseBoolError>>),

    /// The bind address could not be read.
    #[error(transparent)]
    Addr(#[from] envfury::Error<envfury::OrParseError<std::net::AddrParseError>>),
}

impl HttpConfig {
    /// Create config from environment variables.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let enabled = envfury::or_parse("WAYMARK_HTTP_ENABLED", "false")?;
        let addr = envfury::or_parse("WAYMARK_HTTP_ADDR", "0.0.0.0:24119")?;
        Ok(Self { enabled, addr })
    }
}
