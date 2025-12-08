//! Rappel - Distributed & durable background events.
//!
//! This crate provides the server-side orchestration for durable workflow execution.
//! It manages two worker pools:
//! - Action Workers: Execute individual actions (pure functions)
//! - Instance Workers: Run workflow instances with replay

pub mod config;
pub mod coordinator;
pub mod db;
pub mod messages;
pub mod server;
pub mod worker;

pub use config::Config;
pub use coordinator::{HostConfig, HostCoordinator, HostStats};
