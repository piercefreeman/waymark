use std::time::Duration;

/// Configuration for the garbage collector task.
#[derive(Debug, Clone)]
pub struct GarbageCollectorConfig {
    /// How often to run a garbage collection sweep.
    pub interval: Duration,
    /// Maximum number of done instances to delete in one batch.
    pub batch_size: usize,
    /// Retention window for done instances.
    pub retention: Duration,
}

impl Default for GarbageCollectorConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5 * 60),
            batch_size: 100,
            retention: Duration::from_secs(24 * 60 * 60),
        }
    }
}
