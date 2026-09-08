//! Config for the essential-metrics subsystem.

use std::num::{NonZeroU64, NonZeroUsize};

use waymark_nonzero_duration::NonZeroDuration;

/// Configuration for the essential-metrics subsystem.
#[derive(Debug, Clone, Copy)]
pub struct EssentialMetricsConfig {
    /// How often a node samples itself.
    pub sample_interval: NonZeroDuration,

    /// Batching between the sampler and the store sink.
    pub lossy_batcher_policy: waymark_lossy_batcher::ValidPolicy,

    /// How long samples are kept; the retention sweep deletes older ones.
    pub retention: NonZeroDuration,

    /// How often the retention sweep runs.
    pub retention_sweep_interval: NonZeroDuration,
}

/// Error returned when reading an [`EssentialMetricsConfig`] from the
/// environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// An integer-backed variable could not be read.
    #[error(transparent)]
    IntOrDefault(#[from] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),

    /// The lossy batcher policy asks for more concurrent flushes than its
    /// buffers can hold.
    #[error("lossy batcher policy: {0}")]
    LossyBatcherPolicy(#[source] waymark_lossy_batcher::TooManyFlushers),
}

impl EssentialMetricsConfig {
    /// Create config from environment variables.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let sample_interval_millis: NonZeroU64 =
            envfury::or_parse("WAYMARK_ESSENTIAL_METRICS_SAMPLE_INTERVAL_MS", "10000")?;

        let buffers: NonZeroUsize =
            envfury::or_parse("WAYMARK_ESSENTIAL_METRICS_SINK_BUFFERS", "3")?;
        let max_batch: NonZeroUsize =
            envfury::or_parse("WAYMARK_ESSENTIAL_METRICS_SINK_MAX_BATCH", "64")?;
        let max_delay_millis: NonZeroU64 =
            envfury::or_parse("WAYMARK_ESSENTIAL_METRICS_SINK_MAX_DELAY_MS", "5000")?;
        let flushers: NonZeroUsize =
            envfury::or_parse("WAYMARK_ESSENTIAL_METRICS_SINK_FLUSHERS", "1")?;

        // 7 days.
        let retention_millis: NonZeroU64 =
            envfury::or_parse("WAYMARK_ESSENTIAL_METRICS_RETENTION_MS", "604800000")?;

        // 10 minutes.
        let retention_sweep_interval_millis: NonZeroU64 = envfury::or_parse(
            "WAYMARK_ESSENTIAL_METRICS_RETENTION_SWEEP_INTERVAL_MS",
            "600000",
        )?;

        let lossy_batcher_policy = waymark_lossy_batcher::Policy {
            buffers,
            max_batch,
            max_delay: NonZeroDuration::from_nonzero_millis(max_delay_millis),
            flushers,
        }
        .validate()
        .map_err(FromEnvError::LossyBatcherPolicy)?;

        Ok(Self {
            sample_interval: NonZeroDuration::from_nonzero_millis(sample_interval_millis),
            lossy_batcher_policy,
            retention: NonZeroDuration::from_nonzero_millis(retention_millis),
            retention_sweep_interval: NonZeroDuration::from_nonzero_millis(
                retention_sweep_interval_millis,
            ),
        })
    }
}
