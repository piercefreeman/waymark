//! The timing of a migrations lock.

use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

/// The timing of a migrations lock: how often its holder renews it, when
/// it expires unless renewed, and how long before that its holder stops
/// relying on it.
#[derive(Debug, Clone)]
pub struct MigrationsLockTiming {
    /// How long after one renewal the next one is sent.
    pub heartbeat: NonZeroDuration,

    /// How long after a renewal the lock expires, unless renewed again.
    pub time_to_live: NonZeroDuration,

    /// How long before the lock expires its holder stops relying on it:
    /// the time the holder has to stop what it does under the lock.
    pub fencing_margin: NonZeroDuration,
}

/// A [`MigrationsLockTiming`] that passed
/// [`MigrationsLockTiming::validate`]: the only form the migrations lock
/// keeper accepts.
// `Clone` is for the configs that hold a checked timing and the keepers that
// are given one out of a config.
#[derive(Debug, Clone)]
pub struct ValidMigrationsLockTiming {
    /// The validated timing.
    timing: MigrationsLockTiming,

    /// How long after a renewal the holder stops relying on the lock: the
    /// time to live less the fencing margin.
    relied_on_for: Duration,
}

/// Error returned by [`MigrationsLockTiming::validate`].
#[derive(Debug, thiserror::Error)]
pub enum ValidateError {
    /// A renewal confirmed in time would leave no room for the next one:
    /// the holder would stop relying on the lock before, or just as, the
    /// next renewal is sent.
    #[error(
        "heartbeat ({:?}) plus fencing margin ({:?}) must be less than time to live ({:?})",
        .heartbeat.get(),
        .fencing_margin.get(),
        .time_to_live.get()
    )]
    NoRoomForARenewal {
        /// The requested heartbeat.
        heartbeat: NonZeroDuration,

        /// The requested time to live.
        time_to_live: NonZeroDuration,

        /// The requested fencing margin.
        fencing_margin: NonZeroDuration,
    },
}

impl MigrationsLockTiming {
    /// Check that a heartbeat fits in the time the holder relies on the
    /// lock for after a renewal: the heartbeat plus the fencing margin is
    /// less than the time to live.
    pub fn validate(self) -> Result<ValidMigrationsLockTiming, ValidateError> {
        let relied_on_for = self
            .time_to_live
            .get()
            .checked_sub(self.fencing_margin.get());

        let relied_on_for = match relied_on_for {
            Some(relied_on_for) if self.heartbeat.get() < relied_on_for => relied_on_for,
            Some(_) | None => {
                return Err(ValidateError::NoRoomForARenewal {
                    heartbeat: self.heartbeat,
                    time_to_live: self.time_to_live,
                    fencing_margin: self.fencing_margin,
                });
            }
        };

        Ok(ValidMigrationsLockTiming {
            timing: self,
            relied_on_for,
        })
    }
}

impl ValidMigrationsLockTiming {
    /// How long after one renewal the next one is sent.
    pub fn heartbeat(&self) -> NonZeroDuration {
        self.timing.heartbeat
    }

    /// How long after a renewal the lock expires, unless renewed again.
    pub fn time_to_live(&self) -> NonZeroDuration {
        self.timing.time_to_live
    }

    /// How long before the lock expires its holder stops relying on it.
    pub fn fencing_margin(&self) -> NonZeroDuration {
        self.timing.fencing_margin
    }

    /// How long after a renewal the holder stops relying on the lock: the
    /// time to live less the fencing margin.
    pub fn relied_on_for(&self) -> Duration {
        self.relied_on_for
    }
}

#[cfg(test)]
mod tests;
