//! Background garbage collection for old finished workflow instances.

mod task;

pub use task::{GarbageCollectorConfig, GarbageCollectorTask};
