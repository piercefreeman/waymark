use std::time::Duration;

/// Configuration for the scheduler task.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often to poll for due schedules.
    pub poll_interval: Duration,
    /// Maximum number of schedules to process per poll.
    pub batch_size: i32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
        }
    }
}
