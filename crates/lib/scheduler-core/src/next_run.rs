//! Next-run computation.

use std::num::NonZeroU64;

use rand::Rng as _;

use crate::{BeyondEvaluationHorizonError, Schedule, ScheduleDefinition};

/// Compute when the next run of `definition` is due, strictly after
/// `now`, with jitter applied.
///
/// For intervals the base is `now + interval`: callers pass the spawn
/// instant (or, at registration, the registration instant) as `now`.
///
/// `Ok(None)` means the schedule genuinely has no next run: its cron
/// expression matches no instant at all. An error means the next run
/// exists but cannot be produced — the occurrence search ran into the
/// cron evaluation horizon, or the arithmetic left the representable
/// time range.
pub fn compute_next_run(
    definition: &ScheduleDefinition,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, ComputeNextRunError> {
    let base = match &definition.schedule {
        Schedule::CronExpression(expression) => {
            let occurrence = expression
                .next_occurrence_after(now)
                .map_err(ComputeNextRunError::BeyondEvaluationHorizon)?;
            match occurrence {
                Some(occurrence) => occurrence,
                None => return Ok(None),
            }
        }
        Schedule::IntervalSeconds(interval_seconds) => {
            add_seconds(now, *interval_seconds).ok_or(ComputeNextRunError::OutOfRange)?
        }
    };

    if definition.jitter_seconds == 0 {
        return Ok(Some(base));
    }
    let sampled_jitter_seconds = rand::rng().random_range(0..=definition.jitter_seconds);
    let Some(jitter_seconds) = NonZeroU64::new(sampled_jitter_seconds) else {
        return Ok(Some(base));
    };
    match add_seconds(base, jitter_seconds) {
        Some(jittered) => Ok(Some(jittered)),
        None => Err(ComputeNextRunError::OutOfRange),
    }
}

/// The next run exists but could not be produced.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ComputeNextRunError {
    /// The occurrence search ran into the cron evaluation horizon.
    #[error("cron evaluation horizon: {0}")]
    BeyondEvaluationHorizon(#[source] BeyondEvaluationHorizonError),

    /// The next run is beyond the representable time range.
    #[error("the next run is beyond the representable time range")]
    OutOfRange,
}

/// Advance `instant` by `seconds`, `None` on leaving the representable
/// time range.
fn add_seconds(
    instant: chrono::DateTime<chrono::Utc>,
    seconds: NonZeroU64,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let seconds = i64::try_from(seconds.get()).ok()?;
    instant.checked_add_signed(chrono::Duration::try_seconds(seconds)?)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use chrono::TimeZone as _;

    use crate::CronExpression;

    use super::*;

    fn at(hour: u32, minute: u32, second: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .with_ymd_and_hms(2026, 1, 1, hour, minute, second)
            .unwrap()
    }

    fn definition(schedule: Schedule, jitter_seconds: u64) -> ScheduleDefinition {
        ScheduleDefinition {
            schedule,
            jitter_seconds,
            allow_duplicate: false,
        }
    }

    #[test]
    fn cron_advances_to_the_next_occurrence() {
        let definition = definition(
            Schedule::CronExpression(CronExpression::parse("0 * * * *").unwrap()),
            0,
        );
        assert_eq!(
            compute_next_run(&definition, at(12, 34, 56)),
            Ok(Some(at(13, 0, 0)))
        );
    }

    #[test]
    fn interval_advances_from_now() {
        let definition = definition(Schedule::IntervalSeconds(NonZeroU64::new(3600).unwrap()), 0);
        assert_eq!(
            compute_next_run(&definition, at(12, 0, 0)),
            Ok(Some(at(13, 0, 0)))
        );
    }

    #[test]
    fn jitter_stays_within_the_window() {
        let definition = definition(Schedule::IntervalSeconds(NonZeroU64::new(60).unwrap()), 5);
        for _ in 0..64 {
            let next = compute_next_run(&definition, at(12, 0, 0))
                .unwrap()
                .unwrap();
            assert!(next >= at(12, 1, 0));
            assert!(next <= at(12, 1, 5));
        }
    }

    #[test]
    fn never_occurring_cron_expression_has_no_next_run() {
        let definition = definition(
            Schedule::CronExpression(CronExpression::parse("0 0 30 2 *").unwrap()),
            0,
        );
        assert_eq!(compute_next_run(&definition, at(12, 0, 0)), Ok(None));
    }

    #[test]
    fn unrepresentable_arithmetic_is_an_error() {
        let definition = definition(
            Schedule::IntervalSeconds(NonZeroU64::new(u64::MAX).unwrap()),
            0,
        );
        assert_eq!(
            compute_next_run(&definition, at(12, 0, 0)),
            Err(ComputeNextRunError::OutOfRange)
        );
    }
}
