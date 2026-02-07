//! Web application server for the Waymark workflow dashboard.
//!
//! This module provides a human-readable web UI for inspecting workflow instances.
//!
//! The webapp is disabled by default and can be enabled via environment variables:
//! - `WAYMARK_WEBAPP_ENABLED`: Set to "true" or "1" to enable
//! - `WAYMARK_WEBAPP_ADDR`: Address to bind to (default: 0.0.0.0:24119)

mod server;
mod types;

pub use server::WebappServer;
pub use types::*;
