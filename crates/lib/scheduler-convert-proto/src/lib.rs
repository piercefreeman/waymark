//! [`TryConvert`](waymark_convert_core::TryConvert) implementations
//! between the schedule wire messages and the scheduler domain types.
//!
//! Proto terminates at the transport layer, and this crate is where it
//! does: the bridge converts inbound definitions and statuses into
//! `waymark_scheduler_core` types here (the conversion is the validation
//! point — unset oneofs, unparsable cron expressions, and non-positive
//! intervals all fail it), and converts domain values back into wire
//! messages for listing responses. Nothing behind the bridge sees a
//! proto type.

#![warn(missing_docs)]

use std::num::NonZeroU64;

use waymark_convert_core::TryConvert;
use waymark_proto::messages as proto;

/// Stateless converter between schedule wire messages and scheduler
/// domain types.
pub struct Converter;

/// A wire schedule definition was not accepted.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleDefinitionError {
    /// The definition's schedule oneof is unset.
    #[error("the schedule definition has no cron expression or interval")]
    MissingSchedule,

    /// The cron expression was not accepted.
    #[error("invalid cron expression: {0}")]
    InvalidCronExpression(#[source] waymark_scheduler_core::ParseCronExpressionError),

    /// The interval is zero or negative.
    #[error("interval must be positive, got {seconds}")]
    NonPositiveIntervalSeconds {
        /// The interval as given.
        seconds: i64,
    },

    /// The jitter window is negative.
    #[error("jitter must be non-negative, got {seconds}")]
    NegativeJitterSeconds {
        /// The jitter as given.
        seconds: i64,
    },
}

impl TryConvert<&proto::ScheduleDefinition, waymark_scheduler_core::ScheduleDefinition>
    for Converter
{
    type Error = ScheduleDefinitionError;

    fn try_convert(
        from: &proto::ScheduleDefinition,
    ) -> Result<waymark_scheduler_core::ScheduleDefinition, Self::Error> {
        let schedule = match &from.schedule {
            None => return Err(ScheduleDefinitionError::MissingSchedule),
            Some(proto::schedule_definition::Schedule::CronExpression(text)) => {
                let expression = waymark_scheduler_core::CronExpression::parse(text)
                    .map_err(ScheduleDefinitionError::InvalidCronExpression)?;
                waymark_scheduler_core::Schedule::CronExpression(expression)
            }
            Some(proto::schedule_definition::Schedule::IntervalSeconds(seconds)) => {
                let interval_seconds = u64::try_from(*seconds).map_err(|_| {
                    ScheduleDefinitionError::NonPositiveIntervalSeconds { seconds: *seconds }
                })?;
                let interval_seconds = NonZeroU64::new(interval_seconds).ok_or(
                    ScheduleDefinitionError::NonPositiveIntervalSeconds { seconds: *seconds },
                )?;
                waymark_scheduler_core::Schedule::IntervalSeconds(interval_seconds)
            }
        };

        let jitter_seconds = u64::try_from(from.jitter_seconds).map_err(|_| {
            ScheduleDefinitionError::NegativeJitterSeconds {
                seconds: from.jitter_seconds,
            }
        })?;

        Ok(waymark_scheduler_core::ScheduleDefinition {
            schedule,
            jitter_seconds,
            allow_duplicate: from.allow_duplicate,
        })
    }
}

/// A domain schedule definition does not fit the wire message.
#[derive(Debug, thiserror::Error)]
pub enum WireScheduleDefinitionError {
    /// The interval does not fit the wire's signed 64-bit field.
    #[error("interval {seconds} does not fit the wire field")]
    IntervalSecondsOutOfRange {
        /// The interval as held.
        seconds: u64,
    },

    /// The jitter window does not fit the wire's signed 64-bit field.
    #[error("jitter {seconds} does not fit the wire field")]
    JitterSecondsOutOfRange {
        /// The jitter as held.
        seconds: u64,
    },
}

impl TryConvert<&waymark_scheduler_core::ScheduleDefinition, proto::ScheduleDefinition>
    for Converter
{
    type Error = WireScheduleDefinitionError;

    fn try_convert(
        from: &waymark_scheduler_core::ScheduleDefinition,
    ) -> Result<proto::ScheduleDefinition, Self::Error> {
        let schedule = match &from.schedule {
            waymark_scheduler_core::Schedule::CronExpression(expression) => {
                proto::schedule_definition::Schedule::CronExpression(expression.as_str().to_owned())
            }
            waymark_scheduler_core::Schedule::IntervalSeconds(interval_seconds) => {
                let seconds = i64::try_from(interval_seconds.get()).map_err(|_| {
                    WireScheduleDefinitionError::IntervalSecondsOutOfRange {
                        seconds: interval_seconds.get(),
                    }
                })?;
                proto::schedule_definition::Schedule::IntervalSeconds(seconds)
            }
        };

        let jitter_seconds = i64::try_from(from.jitter_seconds).map_err(|_| {
            WireScheduleDefinitionError::JitterSecondsOutOfRange {
                seconds: from.jitter_seconds,
            }
        })?;

        Ok(proto::ScheduleDefinition {
            schedule: Some(schedule),
            jitter_seconds,
            allow_duplicate: from.allow_duplicate,
        })
    }
}

/// The wire status carries no schedule status.
#[derive(Debug, thiserror::Error)]
#[error("schedule status is unspecified")]
pub struct UnspecifiedScheduleStatusError;

impl TryConvert<proto::ScheduleStatus, waymark_scheduler_core::ScheduleStatus> for Converter {
    type Error = UnspecifiedScheduleStatusError;

    fn try_convert(
        from: proto::ScheduleStatus,
    ) -> Result<waymark_scheduler_core::ScheduleStatus, Self::Error> {
        match from {
            proto::ScheduleStatus::Unspecified => Err(UnspecifiedScheduleStatusError),
            proto::ScheduleStatus::Active => Ok(waymark_scheduler_core::ScheduleStatus::Active),
            proto::ScheduleStatus::Paused => Ok(waymark_scheduler_core::ScheduleStatus::Paused),
        }
    }
}

impl TryConvert<waymark_scheduler_core::ScheduleStatus, proto::ScheduleStatus> for Converter {
    type Error = std::convert::Infallible;

    fn try_convert(
        from: waymark_scheduler_core::ScheduleStatus,
    ) -> Result<proto::ScheduleStatus, Self::Error> {
        Ok(match from {
            waymark_scheduler_core::ScheduleStatus::Active => proto::ScheduleStatus::Active,
            waymark_scheduler_core::ScheduleStatus::Paused => proto::ScheduleStatus::Paused,
        })
    }
}

impl TryConvert<chrono::DateTime<chrono::Utc>, prost_wkt_types::Timestamp> for Converter {
    type Error = std::convert::Infallible;

    fn try_convert(
        from: chrono::DateTime<chrono::Utc>,
    ) -> Result<prost_wkt_types::Timestamp, Self::Error> {
        Ok(prost_wkt_types::Timestamp::from(from))
    }
}

#[cfg(test)]
mod tests;
