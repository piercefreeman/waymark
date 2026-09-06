//! Config for the observability-events subsystem.

use std::num::{NonZeroU64, NonZeroUsize};

use waymark_nonzero_duration::NonZeroDuration;

/// Configuration for the observability-events subsystem.
#[derive(Debug, Clone, Copy)]
pub struct ObservabilityEventsConfig {
    /// Batching between the emitter and the store sink.
    pub lossy_batcher_policy: waymark_lossy_batcher::ValidPolicy,

    /// How long events are kept; the retention sweep deletes older ones.
    pub retention: NonZeroDuration,

    /// How often the retention sweep runs.
    pub retention_sweep_interval: NonZeroDuration,
}

/// Error returned when reading an [`ObservabilityEventsConfig`] from the
/// environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// An integer-backed variable could not be read.
    #[error(transparent)]
    IntOrDefault(envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),

    /// The lossy batcher policy asks for more concurrent flushes than its
    /// buffers can hold.
    #[error("lossy batcher policy: {0}")]
    LossyBatcherPolicy(#[source] waymark_lossy_batcher::TooManyFlushers),
}

impl ObservabilityEventsConfig {
    /// Create config from environment variables.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let buffers: NonZeroUsize =
            envfury::or_parse("WAYMARK_OBSERVABILITY_EVENTS_SINK_BUFFERS", "3")
                .map_err(FromEnvError::IntOrDefault)?;
        let max_batch: NonZeroUsize =
            envfury::or_parse("WAYMARK_OBSERVABILITY_EVENTS_SINK_MAX_BATCH", "256")
                .map_err(FromEnvError::IntOrDefault)?;
        let max_delay_millis: NonZeroU64 =
            envfury::or_parse("WAYMARK_OBSERVABILITY_EVENTS_SINK_MAX_DELAY_MS", "1000")
                .map_err(FromEnvError::IntOrDefault)?;
        let flushers: NonZeroUsize =
            envfury::or_parse("WAYMARK_OBSERVABILITY_EVENTS_SINK_FLUSHERS", "1")
                .map_err(FromEnvError::IntOrDefault)?;

        // 7 days.
        let retention_millis: NonZeroU64 =
            envfury::or_parse("WAYMARK_OBSERVABILITY_EVENTS_RETENTION_MS", "604800000")
                .map_err(FromEnvError::IntOrDefault)?;

        // 10 minutes.
        let retention_sweep_interval_millis: NonZeroU64 = envfury::or_parse(
            "WAYMARK_OBSERVABILITY_EVENTS_RETENTION_SWEEP_INTERVAL_MS",
            "600000",
        )
        .map_err(FromEnvError::IntOrDefault)?;

        let lossy_batcher_policy = waymark_lossy_batcher::Policy {
            buffers,
            max_batch,
            max_delay: NonZeroDuration::from_nonzero_millis(max_delay_millis),
            flushers,
        }
        .validate()
        .map_err(FromEnvError::LossyBatcherPolicy)?;

        Ok(Self {
            lossy_batcher_policy,
            retention: NonZeroDuration::from_nonzero_millis(retention_millis),
            retention_sweep_interval: NonZeroDuration::from_nonzero_millis(
                retention_sweep_interval_millis,
            ),
        })
    }
}
