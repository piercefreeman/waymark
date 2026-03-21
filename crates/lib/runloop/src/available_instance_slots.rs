//! The available instance slots privitives.

#![deny(missing_docs, clippy::missing_docs_in_private_items)]

use std::{num::NonZeroUsize, sync::atomic::AtomicUsize};

/// [`Calc`] encapsulates the notion of the available instance slots.
/// It is responsible for providing the computations necessary to obtain
/// the amount of available instance slots based on the max concurrent
/// instances.
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
pub struct Tracker {
    /// Calculation logic and parameters for computing the available slots.
    pub calc: Calc,

    /// The materialized copy of the pre-computed available slots value.
    pub available_slots: AtomicUsize,
}

impl Calc {
    /// Get the amount of available slots by discounting the active instances
    /// from the max number of instances.
    /// Saturates to `0` if `active_instances` exceeds
    /// the configured `max_concurrent_instances`.
    pub fn discounting_active_saturating(&self, active_instances: usize) -> usize {
        self.max_concurrent_instances
            .get()
            .saturating_sub(active_instances)
    }

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

impl Tracker {
    /// Create a new tracker with no active instances.
    pub fn from_scratch(calc: Calc) -> Self {
        // Should always succeed.
        Self::with_active_instances(calc, 0).unwrap()
    }

    /// Create a new tracker with the specified amount of active instances.
    pub fn with_active_instances(calc: Calc, active_instances: usize) -> Option<Self> {
        let available_slots = calc.discounting_active_checked(active_instances)?;
        let available_slots = AtomicUsize::new(available_slots);
        Some(Self {
            calc,
            available_slots,
        })
    }

    /// Store raw available slots value.
    fn store_raw(&self, available_slots: usize) {
        self.available_slots
            .store(available_slots, std::sync::atomic::Ordering::SeqCst);
    }

    /// Update the available slots by discounting the provided active instances
    /// value.
    /// Returns `None` if the number of active instances exceeds the capacity.
    #[allow(dead_code)]
    pub fn update_checked(&self, active_instances: usize) -> Option<()> {
        let available_slots = self.calc.discounting_active_checked(active_instances)?;
        self.store_raw(available_slots);
        Some(())
    }

    /// Update the available slots by discounting the provided active instances
    /// value.
    /// Saturates to `0` if the number of active instances exceeds the capacity.
    pub fn update_saturating(&self, active_instances: usize) {
        let available_slots = self.calc.discounting_active_saturating(active_instances);
        self.store_raw(available_slots);
    }

    /// Get the currently stored value of available instance slot amount.
    pub fn get(&self) -> usize {
        self.available_slots
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
