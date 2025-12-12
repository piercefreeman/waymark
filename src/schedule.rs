//! Cron and interval schedule utilities.
//!
//! This module provides utilities for computing the next run time for
//! cron expressions and fixed intervals.

use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;

/// Compute the next run time for a cron expression.
///
/// Returns the next occurrence after the current time (UTC).
pub fn next_cron_run(cron_expr: &str) -> Result<DateTime<Utc>, String> {
    let schedule = Schedule::from_str(cron_expr)
        .map_err(|e| format!("Invalid cron expression '{}': {}", cron_expr, e))?;
    schedule
        .upcoming(Utc)
        .next()
        .ok_or_else(|| "No upcoming schedule found".to_string())
}

/// Compute the next run time for an interval-based schedule.
///
/// If `last_run_at` is provided, the next run is `last_run_at + interval_seconds`.
/// Otherwise, the next run is `now + interval_seconds`.
pub fn next_interval_run(
    interval_seconds: i64,
    last_run_at: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    let base = last_run_at.unwrap_or_else(Utc::now);
    base + chrono::Duration::seconds(interval_seconds)
}

/// Validate a cron expression without computing the next run.
pub fn validate_cron(cron_expr: &str) -> Result<(), String> {
    Schedule::from_str(cron_expr)
        .map(|_| ())
        .map_err(|e| format!("Invalid cron expression '{}': {}", cron_expr, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_cron_expression() {
        // Every hour at minute 0
        assert!(validate_cron("0 * * * *").is_ok());
        // Every day at midnight
        assert!(validate_cron("0 0 * * *").is_ok());
        // Every minute
        assert!(validate_cron("* * * * *").is_ok());
    }

    #[test]
    fn test_invalid_cron_expression() {
        assert!(validate_cron("invalid").is_err());
        assert!(validate_cron("").is_err());
        assert!(validate_cron("0 0 0 0 0 0 0 0").is_err());
    }

    #[test]
    fn test_next_cron_run() {
        // Every minute should return a time in the future
        let next = next_cron_run("* * * * *").unwrap();
        assert!(next > Utc::now());
    }

    #[test]
    fn test_next_interval_run_from_now() {
        let before = Utc::now();
        let next = next_interval_run(3600, None);
        let after = Utc::now();

        // Should be approximately 1 hour from now
        assert!(next >= before + chrono::Duration::seconds(3600));
        assert!(next <= after + chrono::Duration::seconds(3600));
    }

    #[test]
    fn test_next_interval_run_from_last() {
        let last_run = Utc::now() - chrono::Duration::seconds(1800);
        let next = next_interval_run(3600, Some(last_run));

        // Should be 1 hour after last_run (30 minutes from now)
        let expected = last_run + chrono::Duration::seconds(3600);
        assert_eq!(next, expected);
    }
}
