//! Waymark - worker pool infrastructure plus the core IR/runtime port.

pub mod config;
pub mod garbage_collector;
pub mod scheduler;
pub mod waymark_core;

// Worker infrastructure (preserved from the legacy Rust core).
pub use garbage_collector::{GarbageCollectorConfig, GarbageCollectorTask};
pub use scheduler::{SchedulerConfig, SchedulerTask};
