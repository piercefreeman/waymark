//! The schedule definition: a schedule's complete policy.

use std::num::NonZeroU64;

use crate::CronExpression;

/// A schedule's complete policy: when runs happen and how overlap with a
/// still-running previous instance is treated.
///
/// This is the domain form of the persisted definition blob.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScheduleDefinition {
    /// When runs happen.
    pub schedule: Schedule,

    /// Random delay in `0..=jitter_seconds` added to each computed run.
    pub jitter_seconds: u64,

    /// If false, a due run is skipped (the schedule still advances)
    /// while the previous instance is still running.
    pub allow_duplicate: bool,
}

/// When a schedule's runs happen.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Schedule {
    /// At each occurrence of a cron expression.
    CronExpression(CronExpression),

    /// At a fixed interval after each run.
    IntervalSeconds(NonZeroU64),
}
