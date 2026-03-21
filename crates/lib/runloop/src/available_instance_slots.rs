//! The available instance slots tracker.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use std::{num::NonZeroUsize, sync::Arc};

/// [`Tracker`] keeps track of the number of available instances, and allows
/// waiting and reserving them.
#[derive(Debug)]
pub struct Tracker {
    /// Internal semaphore.
    ///
    /// Private, since making it pub would allow adding permits, which would
    /// break the assumed invariants.
    semaphore: Arc<tokio::sync::Semaphore>,
}

/// A non-zero reserve, guaranteed to hold a non-zero amount of permits.
#[derive(Debug)]
pub struct NonZeroReserve {
    /// The internal permit, guanranteed to be non-zero by construction.
    permit: tokio::sync::OwnedSemaphorePermit,
}

/// An error when acquiring the permit from a closed semaphore.
#[derive(Debug, thiserror::Error)]
#[error("semaphore closed")]
pub struct AcquireError;

/// An error when not holding enough permits to give them back.
#[derive(Debug, thiserror::Error)]
#[error("not holding enough permits to give {to_give_back} back")]
pub struct NotHoldingEnoughToGiveBackError {
    /// The amount of permits that were attempted to give back.
    pub to_give_back: NonZeroUsize,
}

impl Tracker {
    /// Create a new tracker with the provided maximum number of concurrent
    /// instances.
    pub fn new(max_concurrent_instances: NonZeroUsize) -> Self {
        let semaphore = tokio::sync::Semaphore::new(max_concurrent_instances.get());
        let semaphore = semaphore.into();
        Self { semaphore }
    }

    /// Wait for at least one slot to become available and then reserve all
    /// currently available slots.
    pub async fn reserve(&self) -> Result<NonZeroReserve, AcquireError> {
        // Wait and acquire at least one permit.
        let mut permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AcquireError)?;

        // This is a quite crude and inefficient implementation, but this will
        // do for now.
        // TODO: implement a custom Semaphore for this that allows consuming
        // up to `n` permits atomically (as opposed to exactly `n` like what
        // `tokio::sync::Semaphore::try_acquire_many{_owned}` do).
        loop {
            let available_permits = self.semaphore.available_permits();
            if available_permits == 0 {
                break;
            }

            let result = self
                .semaphore
                .clone()
                .try_acquire_many_owned(available_permits as u32);
            let next_permit = match result {
                Ok(permit) => permit,
                Err(tokio::sync::TryAcquireError::Closed) => return Err(AcquireError),
                Err(tokio::sync::TryAcquireError::NoPermits) => continue,
            };

            permit.merge(next_permit);
            break;
        }

        Ok(NonZeroReserve { permit })
    }
}

impl NonZeroReserve {
    /// Return how many slots are currently reserved by this reserve.
    pub fn reserved_slots(&self) -> NonZeroUsize {
        self.permit.num_permits().try_into().unwrap()
    }

    /// Return `to_give_back` reserved slots back to the tracker.
    pub fn give_back(
        &mut self,
        to_give_back: NonZeroUsize,
    ) -> Result<(), NotHoldingEnoughToGiveBackError> {
        let permits_to_give_back = self
            .permit
            .split(to_give_back.get())
            .ok_or(NotHoldingEnoughToGiveBackError { to_give_back })?;
        drop(permits_to_give_back);
        Ok(())
    }

    /// Merge another non-zero reserve into this one.
    pub fn merge(&mut self, other: NonZeroReserve) {
        self.permit.merge(other.permit);
    }
}
