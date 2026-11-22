//! Config for the migrations lock of a Postgres database.

use waymark_migrations_engine::migrations_lock_timing::ValidateError;
use waymark_migrations_engine::{MigrationsLockTiming, ValidMigrationsLockTiming};
use waymark_nonzero_duration::NonZeroDuration;

use crate::IntFromEnvError;

/// Configuration for the migrations lock of one Postgres database.
#[derive(Debug, Clone)]
pub struct Config {
    /// The lock's timing: how often it is renewed, when it expires unless
    /// renewed, and how long before that its holder stops relying on it.
    pub timing: ValidMigrationsLockTiming,

    /// How long one statement on the lock's connection may run before the
    /// server stops it; no limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}

/// Where a migrations lock's config comes from: the prefix of its
/// environment variables and the defaults for the ones not set.
///
/// The variables are `WAYMARK_{PREFIX}_HEARTBEAT_MS`,
/// `WAYMARK_{PREFIX}_TTL_MS`, `WAYMARK_{PREFIX}_FENCING_MARGIN_MS` (`0` is
/// refused for each of the three) and
/// `WAYMARK_{PREFIX}_STATEMENT_TIMEOUT_MS` (`0` turns the timeout off).
pub trait Profile {
    /// The variables' prefix, without the `WAYMARK_`.
    const PREFIX: &'static str;

    /// The heartbeat when its variable is not set.
    const HEARTBEAT_DEFAULT: NonZeroDuration;

    /// The time to live when its variable is not set.
    const TIME_TO_LIVE_DEFAULT: NonZeroDuration;

    /// The fencing margin when its variable is not set.
    const FENCING_MARGIN_DEFAULT: NonZeroDuration;

    /// The statement timeout when its variable is not set; no limit when
    /// `None`.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration>;
}

/// Error returned by [`Config::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The lock's heartbeat could not be read.
    #[error("heartbeat: {0}")]
    Heartbeat(#[source] IntFromEnvError),

    /// The lock's time to live could not be read.
    #[error("time to live: {0}")]
    TimeToLive(#[source] IntFromEnvError),

    /// The lock's fencing margin could not be read.
    #[error("fencing margin: {0}")]
    FencingMargin(#[source] IntFromEnvError),

    /// The statement timeout of the lock's connection could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] IntFromEnvError),

    /// The three durations do not make a valid timing together.
    #[error("timing: {0}")]
    Timing(#[source] ValidateError),
}

impl Config {
    /// Create config from the environment variables of the profile
    /// `Profile`.
    pub fn from_env<Profile: self::Profile>() -> Result<Self, FromEnvError> {
        let heartbeat = envfury::maybe(format!("WAYMARK_{}_HEARTBEAT_MS", Profile::PREFIX))
            .map_err(FromEnvError::Heartbeat)?
            .map_or(
                Profile::HEARTBEAT_DEFAULT,
                NonZeroDuration::from_nonzero_millis,
            );
        let time_to_live = envfury::maybe(format!("WAYMARK_{}_TTL_MS", Profile::PREFIX))
            .map_err(FromEnvError::TimeToLive)?
            .map_or(
                Profile::TIME_TO_LIVE_DEFAULT,
                NonZeroDuration::from_nonzero_millis,
            );
        let fencing_margin =
            envfury::maybe(format!("WAYMARK_{}_FENCING_MARGIN_MS", Profile::PREFIX))
                .map_err(FromEnvError::FencingMargin)?
                .map_or(
                    Profile::FENCING_MARGIN_DEFAULT,
                    NonZeroDuration::from_nonzero_millis,
                );
        let statement_timeout = crate::statement_timeout_from_env(Profile::PREFIX)
            .map_err(FromEnvError::StatementTimeout)?
            .unwrap_or(Profile::STATEMENT_TIMEOUT_DEFAULT);

        let timing = MigrationsLockTiming {
            heartbeat,
            time_to_live,
            fencing_margin,
        };
        let timing = timing.validate().map_err(FromEnvError::Timing)?;

        Ok(Self {
            timing,
            statement_timeout,
        })
    }
}

#[cfg(test)]
mod tests;
