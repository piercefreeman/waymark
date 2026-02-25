//! Shared types for the webapp server.

/// Configuration for the webapp server.
#[derive(Debug, Clone)]
pub struct WebappConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
}

impl Default for WebappConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "0.0.0.0".to_string(),
            port: 24119,
        }
    }
}

impl WebappConfig {
    /// Create config from environment variables.
    pub fn from_env() -> Self {
        let enabled = std::env::var("WAYMARK_WEBAPP_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let (host, port) = std::env::var("WAYMARK_WEBAPP_ADDR")
            .ok()
            .and_then(|addr| {
                let parts: Vec<&str> = addr.split(':').collect();
                if parts.len() == 2 {
                    let host = parts[0].to_string();
                    let port = parts[1].parse().ok()?;
                    Some((host, port))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| ("0.0.0.0".to_string(), 24119));

        Self {
            enabled,
            host,
            port,
        }
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
