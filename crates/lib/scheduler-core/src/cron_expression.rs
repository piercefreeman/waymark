//! Validated cron expressions.

/// A validated cron expression.
///
/// Any value of this type is guaranteed parseable: [`CronExpression::parse`]
/// is the only constructor, and deserialization re-runs it, so a value
/// decoded from persisted bytes carries the same guarantee.
///
/// The original text is kept as written — a 5-field expression is not
/// rewritten to the 6-field form used for evaluation — so display and
/// serialization round-trip exactly. Equality is textual: two expressions
/// describing the same occurrences but written differently compare
/// unequal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CronExpression {
    text: String,
}

impl CronExpression {
    /// Validate `text` as a cron expression.
    ///
    /// Standard 5-field Unix expressions (minute, hour, day-of-month,
    /// month, day-of-week) are accepted and evaluated at second 0 of
    /// each match; 6-field expressions with a seconds column are
    /// accepted as-is. 7-field expressions with a year column are
    /// rejected: the cron library's year evaluation is unreliable, so
    /// year-bounded expressions must be unrepresentable.
    pub fn parse(text: &str) -> Result<Self, ParseCronExpressionError> {
        if text.split_whitespace().count() >= 7 {
            return Err(ParseCronExpressionError::YearFieldUnsupported {
                text: text.to_owned(),
            });
        }
        match <cron::Schedule as std::str::FromStr>::from_str(&normalize(text)) {
            Ok(_) => Ok(Self {
                text: text.to_owned(),
            }),
            Err(source) => Err(ParseCronExpressionError::Invalid {
                text: text.to_owned(),
                source,
            }),
        }
    }

    /// The expression as written.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The first occurrence strictly after `after`.
    ///
    /// `Ok(None)` means the expression genuinely has no occurrences: it
    /// matches no instant at all (like a February 30). An error means
    /// the search ran into the cron library's evaluation horizon
    /// instead — occurrences beyond it are undecidable here.
    pub fn next_occurrence_after(
        &self,
        after: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, BeyondEvaluationHorizonError> {
        let schedule = <cron::Schedule as std::str::FromStr>::from_str(&normalize(&self.text))
            .expect("the expression was validated at construction");
        if let Some(occurrence) = schedule.after(&after).next() {
            return Ok(Some(occurrence));
        }

        // The search found nothing. An expression matching no instant at
        // all is genuinely empty. Otherwise it is calendar-periodic (year
        // fields are rejected at parse) with known occurrences, so the
        // only possible cause is the library's evaluation horizon.
        if schedule.after(&EPOCH).next().is_none() {
            Ok(None)
        } else {
            Err(BeyondEvaluationHorizonError {
                expression: self.text.clone(),
            })
        }
    }
}

/// A fixed probe origin for deciding whether an expression matches any
/// instant at all; any in-range instant works, since the probe only
/// distinguishes "some occurrence exists" from "none ever".
const EPOCH: chrono::DateTime<chrono::Utc> = chrono::DateTime::UNIX_EPOCH;

impl std::fmt::Display for CronExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl TryFrom<String> for CronExpression {
    type Error = ParseCronExpressionError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text)
    }
}

impl From<CronExpression> for String {
    fn from(expression: CronExpression) -> Self {
        expression.text
    }
}

/// Convert a 5-field Unix cron expression to the 6-field form the `cron`
/// crate evaluates, by prepending a seconds column pinned to 0. Any other
/// field count passes through unchanged.
fn normalize(text: &str) -> String {
    if text.split_whitespace().count() == 5 {
        format!("0 {text}")
    } else {
        text.to_owned()
    }
}

/// The text was not accepted as a cron expression.
#[derive(Debug, thiserror::Error)]
pub enum ParseCronExpressionError {
    /// The text failed to parse.
    #[error("invalid cron expression {text:?}: {source}")]
    Invalid {
        /// The text as given.
        text: String,

        /// The parse failure.
        #[source]
        source: cron::error::Error,
    },

    /// The expression has a year column. Year-bounded expressions are
    /// rejected: the cron library's year evaluation is unreliable, so
    /// they must be unrepresentable.
    #[error("year field in cron expression {text:?} is not supported")]
    YearFieldUnsupported {
        /// The text as given.
        text: String,
    },
}

/// The occurrence search ran into the cron library's evaluation horizon
/// (it evaluates years 1970 through 2100): no occurrence was found
/// within it, and existence beyond it is undecidable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no occurrence of {expression:?} within the cron evaluation horizon (through year 2100)")]
pub struct BeyondEvaluationHorizonError {
    /// The expression whose search hit the horizon.
    pub expression: String,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    #[test]
    fn accepts_five_field_expressions() {
        for text in ["* * * * *", "0 * * * *", "*/15 * * * *", "0 0 * * *"] {
            let expression = CronExpression::parse(text).unwrap();
            assert_eq!(expression.as_str(), text);
        }
    }

    #[test]
    fn accepts_six_field_expressions() {
        let expression = CronExpression::parse("30 0 * * * *").unwrap();
        assert_eq!(expression.as_str(), "30 0 * * * *");
    }

    #[test]
    fn rejects_invalid_expressions() {
        assert!(CronExpression::parse("invalid").is_err());
        assert!(CronExpression::parse("").is_err());
        assert!(CronExpression::parse("61 * * * *").is_err());
    }

    #[test]
    fn five_field_expressions_match_at_second_zero() {
        let expression = CronExpression::parse("* * * * *").unwrap();
        let after = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 30).unwrap();
        let next = expression.next_occurrence_after(after).unwrap().unwrap();
        assert_eq!(
            next,
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap()
        );
    }

    #[test]
    fn hourly_expression_advances_to_the_next_hour() {
        let expression = CronExpression::parse("0 * * * *").unwrap();
        let after = chrono::Utc
            .with_ymd_and_hms(2026, 1, 1, 12, 34, 56)
            .unwrap();
        let next = expression.next_occurrence_after(after).unwrap().unwrap();
        assert_eq!(
            next,
            chrono::Utc.with_ymd_and_hms(2026, 1, 1, 13, 0, 0).unwrap()
        );
    }

    #[test]
    fn never_matching_expression_is_exhausted() {
        // February 30 does not exist; the expression matches no instant.
        let expression = CronExpression::parse("0 0 30 2 *").unwrap();
        let after = chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(expression.next_occurrence_after(after), Ok(None));
    }

    #[test]
    fn rejects_year_bounded_expressions() {
        assert!(matches!(
            CronExpression::parse("0 0 0 1 1 * 2090"),
            Err(ParseCronExpressionError::YearFieldUnsupported { .. })
        ));
    }

    #[test]
    fn periodic_expression_past_the_horizon_is_an_error() {
        let expression = CronExpression::parse("* * * * *").unwrap();
        let after = chrono::Utc.with_ymd_and_hms(2101, 1, 1, 0, 0, 0).unwrap();
        assert!(expression.next_occurrence_after(after).is_err());
    }

    #[test]
    fn conversion_round_trips_the_original_text() {
        let expression = CronExpression::try_from("0 * * * *".to_owned()).unwrap();
        assert_eq!(String::from(expression), "0 * * * *");
    }

    #[test]
    fn conversion_rejects_invalid_text() {
        assert!(CronExpression::try_from("nope".to_owned()).is_err());
    }
}
