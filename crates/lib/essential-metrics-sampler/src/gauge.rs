//! A last-set gauge cell.

use std::sync::atomic::{AtomicU64, Ordering};

/// A last-set gauge cell (f64 bits): written through the `metrics`
/// handle, read at sampling time.
#[derive(Debug, Default)]
pub struct Cell(AtomicU64);

impl Cell {
    /// The current value.
    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

impl metrics::GaugeFn for Cell {
    fn increment(&self, value: f64) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bits| {
                Some((f64::from_bits(bits) + value).to_bits())
            });
    }

    fn decrement(&self, value: f64) {
        self.increment(-value);
    }

    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}
