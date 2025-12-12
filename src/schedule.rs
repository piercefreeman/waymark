//! Cron and interval schedule utilities.
//!
//! This module provides utilities for computing the next run time for
//! cron expressions and fixed intervals.
//!
//! Note: This module accepts standard 5-field Unix cron expressions
//! (minute, hour, day-of-month, month, day-of-week) and converts them
//! to 6-field format (with seconds) for the `cron` crate.

use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;

/// Convert a 5-field Unix cron expression to 6-field format.
///
/// The `cron` crate requires 6 fields (sec min hour dom month dow),
/// but standard Unix cron uses 5 fields (min hour dom month dow).
/// This function prepends "0 " to run at second 0 of each match.
fn normalize_cron_expr(cron_expr: &str) -> String {
    let fields: Vec<&str> = cron_expr.split_whitespace().collect();
    if fields.len() == 5 {
        // Standard 5-field cron: prepend "0" for seconds
        format!("0 {}", cron_expr)
    } else {
        // Already 6+ fields, use as-is
        cron_expr.to_string()
    }
}

/// Compute the next run time for a cron expression.
///
/// Accepts standard 5-field Unix cron expressions (e.g., "0 * * * *" for hourly)
/// or 6-field expressions with seconds.
///
/// Returns the next occurrence after the current time (UTC).
pub fn next_cron_run(cron_expr: &str) -> Result<DateTime<Utc>, String> {
    let normalized = normalize_cron_expr(cron_expr);
    let schedule = Schedule::from_str(&normalized)
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
///
/// Accepts standard 5-field Unix cron expressions or 6-field expressions.
pub fn validate_cron(cron_expr: &str) -> Result<(), String> {
    let normalized = normalize_cron_expr(cron_expr);
    Schedule::from_str(&normalized)
        .map(|_| ())
        .map_err(|e| format!("Invalid cron expression '{}': {}", cron_expr, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_cron_expr() {
        // 5-field should get "0 " prepended
        assert_eq!(normalize_cron_expr("* * * * *"), "0 * * * * *");
        assert_eq!(normalize_cron_expr("0 * * * *"), "0 0 * * * *");

        // 6-field should remain unchanged
        assert_eq!(normalize_cron_expr("0 0 * * * *"), "0 0 * * * *");
    }

    #[test]
    fn test_valid_cron_expression() {
        // Standard 5-field Unix cron expressions
        // Every hour at minute 0
        assert!(validate_cron("0 * * * *").is_ok());
        // Every day at midnight
        assert!(validate_cron("0 0 * * *").is_ok());
        // Every minute
        assert!(validate_cron("* * * * *").is_ok());

        // 6-field expression with seconds
        assert!(validate_cron("0 0 * * * *").is_ok());
    }

    #[test]
    fn test_invalid_cron_expression() {
        assert!(validate_cron("invalid").is_err());
        assert!(validate_cron("").is_err());
        // Too many fields
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
