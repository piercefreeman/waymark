//! The available instance slots privitives.

use std::{num::NonZeroUsize, sync::atomic::AtomicUsize};

/// [`Calc`] encapsulates the notion of the available instance slots.
/// It is responsible for providing the computations necessary to .
pub struct Calc {
    pub max_concurrent_instances: NonZeroUsize,
}

/// [`Tracker`] encapsulates an available instance slots calculation logic and
/// a matrialized copy of computed available instance slots.
///
/// It also provies access to the available instance slots in an atomic and
/// thread-safe way.
pub struct Tracker {
    pub calc: Calc,
    pub available_slots: AtomicUsize,
}

impl Calc {
    /// Create a new [`Calc`] instance with the `max_concurrent_instances`
    /// initialized to the provided value or 1.
    #[deprecated = "prefer accepting the actual NonZeroUsize as user input"]
    pub fn new_saturating(max_concurrent_instances: usize) -> Self {
        let max_concurrent_instances = NonZeroUsize::new(max_concurrent_instances)
            .unwrap_or_else(|| unsafe { NonZeroUsize::new_unchecked(1) });

        Self {
            max_concurrent_instances,
        }
    }

    /// Get the amount of available slots by discounting the active intances
    /// from the max number of instances.
    pub fn discounting_active(&self, active_instances: usize) -> usize {
        self.max_concurrent_instances
            .get()
            .saturating_sub(active_instances)
    }
}

impl Tracker {
    pub fn from_scratch(calc: Calc) -> Self {
        Self::with_active_instances(calc, 0)
    }

    pub fn with_active_instances(calc: Calc, active_instances: usize) -> Self {
        let available_slots = calc.discounting_active(active_instances);
        let available_slots = AtomicUsize::new(available_slots);
        Self {
            calc,
            available_slots,
        }
    }

    fn store_raw(&self, available_slots: usize) {
        self.available_slots
            .store(available_slots, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn update(&self, active_instances: usize) {
        let available_slots = self.calc.discounting_active(active_instances);
        self.store_raw(available_slots)
    }

    pub fn get(&self) -> usize {
        self.available_slots
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
