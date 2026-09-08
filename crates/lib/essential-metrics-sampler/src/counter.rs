//! A cumulative counter cell.

use std::sync::atomic::{AtomicU64, Ordering};

/// A cumulative counter cell: incremented through the `metrics` handle,
/// read at sampling time.
#[derive(Debug, Default)]
pub struct Cell(AtomicU64);

impl Cell {
    /// The current count.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl metrics::CounterFn for Cell {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}
