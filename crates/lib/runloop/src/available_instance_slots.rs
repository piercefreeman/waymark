//! The available instance slots privitives.

#![deny(missing_docs, clippy::missing_docs_in_private_items)]

use std::num::NonZeroUsize;

/// [`Calc`] encapsulates the notion of the available instance slots.
/// It is responsible for providing the computations necessary to obtain
/// the amount of available instance slots based on the max concurrent
/// instances.
#[derive(Debug)]
pub struct Calc {
    /// The max amount of concurrent instances.
    /// At least one instance is required.
    pub max_concurrent_instances: NonZeroUsize,
}

/// [`Tracker`] encapsulates an available instance slots calculation logic and
/// a matrialized copy of computed available instance slots.
///
/// It also provides access to the available instance slots in an atomic and
/// thread-safe way.
#[derive(Debug)]
pub struct Tracker {
    /// Calculation logic and parameters for computing the available slots.
    pub calc: Calc,

    /// The handle for updating the available slots.
    tx: tokio::sync::watch::Sender<usize>,
}

/// Read handle for observing available instance slots.
#[derive(Debug, Clone)]
pub struct Reader {
    /// The materialized copy of the pre-computed available slots value.
    rx: tokio::sync::watch::Receiver<usize>,
}

impl Calc {
    /// Get the amount of available slots by discounting the active instances
    /// from the max number of instances.
    /// Returns `None` if `active_instances` exceeds
    /// the configured `max_concurrent_instances`.
    pub fn discounting_active_checked(&self, active_instances: usize) -> Option<usize> {
        self.max_concurrent_instances
            .get()
            .checked_sub(active_instances)
    }
}

/// Errors that can happen while updating the tracked slot count.
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    /// Active instances exceed configured capacity.
    #[error("the amount of active instances exceed the capacity")]
    ExceedsCapacity,

    /// All reader handles were dropped so updates cannot be published.
    #[error("all readers have been dropped")]
    NoReaders,
}

/// Indicates the tracker side was dropped while waiting for updates.
#[derive(Debug, thiserror::Error)]
#[error("tracker has been dropped")]
pub struct TrackerGoneError;

impl Tracker {
    /// Create a new tracker with no active instances.
    pub fn from_scratch(calc: Calc) -> (Self, Reader) {
        // Should always succeed.
        Self::with_active_instances(calc, 0).unwrap()
    }

    /// Create a new tracker with the specified amount of active instances.
    pub fn with_active_instances(calc: Calc, active_instances: usize) -> Option<(Self, Reader)> {
        let available_slots = calc.discounting_active_checked(active_instances)?;

        let (tx, rx) = tokio::sync::watch::channel(available_slots);

        let tracker = Self { calc, tx };

        let reader = Reader { rx };

        Some((tracker, reader))
    }

    /// Update the available slots by discounting the provided active instances
    /// value.
    /// Returns `None` if the number of active instances exceeds the capacity.
    pub fn update(&self, active_instances: usize) -> Result<(), UpdateError> {
        let available_slots = self
            .calc
            .discounting_active_checked(active_instances)
            .ok_or(UpdateError::ExceedsCapacity)?;

        self.tx
            .send(available_slots)
            .map_err(|_| UpdateError::NoReaders)
    }
}

impl Reader {
    /// Wait until at least one instance slot is available.
    pub async fn wait_available(&mut self) -> Result<NonZeroUsize, TrackerGoneError> {
        let val = {
            let val_ref = self
                .rx
                .wait_for(|available| *available > 0)
                .await
                .map_err(|_| TrackerGoneError)?;

            *val_ref
        };

        // Guaranteed to be nonzero by the `wait_for` predicate above.
        let val = NonZeroUsize::new(val).unwrap();

        Ok(val)
    }
}
