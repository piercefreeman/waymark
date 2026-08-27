//! The schedule lifecycle status.

/// The lifecycle status of a schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleStatus {
    /// Due runs spawn VMs.
    Active,

    /// The schedule is retained but never due.
    Paused,
}

impl ScheduleStatus {
    /// The stable textual form, as persisted.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
        }
    }
}

impl std::fmt::Display for ScheduleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ScheduleStatus {
    type Err = ParseScheduleStatusError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            _ => Err(ParseScheduleStatusError {
                text: text.to_owned(),
            }),
        }
    }
}

/// The text does not name a schedule status.
#[derive(Debug, thiserror::Error)]
#[error("unknown schedule status {text:?}")]
pub struct ParseScheduleStatusError {
    /// The text as given.
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_the_textual_form() {
        for status in [ScheduleStatus::Active, ScheduleStatus::Paused] {
            assert_eq!(status.as_str().parse::<ScheduleStatus>().unwrap(), status);
        }
    }

    #[test]
    fn rejects_unknown_text() {
        assert!("deleted".parse::<ScheduleStatus>().is_err());
        assert!("".parse::<ScheduleStatus>().is_err());
    }
}
