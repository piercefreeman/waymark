//! Web application server for the Rappel workflow dashboard.
//!
//! This module provides a human-readable web UI for inspecting workflow instances.
//!
//! The webapp is disabled by default and can be enabled via environment variables:
//! - `RAPPEL_WEBAPP_ENABLED`: Set to "true" or "1" to enable
//! - `RAPPEL_WEBAPP_ADDR`: Address to bind to (default: 0.0.0.0:24119)

mod server;
mod types;

pub use server::WebappServer;
pub use types::*;
