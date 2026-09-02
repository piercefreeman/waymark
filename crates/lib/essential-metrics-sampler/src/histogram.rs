//! A histogram cell backed by counts over fixed bucket boundaries.

use std::sync::atomic::{AtomicU64, Ordering};

/// A histogram cell over `bounds`: each observation finds its bucket and
/// increments it, and sampling drains the counts.
///
/// `N` is the number of counts, one more than the number of bounds — the
/// extra one is the bucket above the last bound, which no bound names.
///
/// Counts are kept disjoint — one increment per observation, whichever
/// bucket it lands in — and turned cumulative only on the way out, where
/// the `less than or equal` form is wanted. Recording stays a single
/// atomic add either way; making the counts cumulative in place would
/// cost an add per bucket above the observation.
#[derive(Debug)]
pub struct Cell<const N: usize> {
    bounds: &'static [f64],
    counts: [AtomicU64; N],
    /// The observed values added together, as `f64` bits.
    sum: AtomicU64,
}

impl<const N: usize> Cell<N> {
    /// A cell bucketing observations by `bounds`.
    ///
    /// # Panics
    ///
    /// If `bounds` does not have exactly one fewer entry than there are
    /// counts. Both come from the same pair of constants, so this fires
    /// at startup or never.
    pub fn new(bounds: &'static [f64]) -> Self {
        assert_eq!(
            bounds.len() + 1,
            N,
            "a histogram cell has one more count than it has bounds",
        );
        Self {
            bounds,
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            sum: AtomicU64::new(0),
        }
    }

    /// The observations recorded since the previous call, which this call
    /// ends: the counts are taken and left at zero, so the next caller
    /// sees a fresh interval.
    ///
    /// The counts are read one bucket at a time rather than all at once,
    /// so an observation recorded during the drain lands either in the
    /// interval closing or in the one opening — never in both, and never
    /// in neither.
    pub fn drain(&self) -> waymark_essential_metrics_core::BucketedHistogram<N> {
        let mut counts = [0_u64; N];
        let mut cumulative = 0_u64;
        for (slot, count) in self.counts.iter().zip(counts.iter_mut()) {
            cumulative += slot.swap(0, Ordering::Relaxed);
            *count = cumulative;
        }
        waymark_essential_metrics_core::BucketedHistogram {
            counts,
            sum: f64::from_bits(self.sum.swap(0, Ordering::Relaxed)),
        }
    }

    /// Add `value` to the running sum, which holds an `f64` in an atomic's
    /// bits and so has to be read, added to and swapped back as one.
    fn add_to_sum(&self, value: f64) {
        let mut observed = self.sum.load(Ordering::Relaxed);
        loop {
            let replacement = (f64::from_bits(observed) + value).to_bits();
            match self.sum.compare_exchange_weak(
                observed,
                replacement,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }
}

impl<const N: usize> metrics::HistogramFn for Cell<N> {
    fn record(&self, value: f64) {
        // Neither a NaN nor an infinity is a duration this records, and
        // either would carry the sum away with it.
        if !value.is_finite() {
            return;
        }
        // Anything above the last bound belongs to the count past the
        // end of them.
        let bucket = self
            .bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(N - 1);
        self.counts[bucket].fetch_add(1, Ordering::Relaxed);
        self.add_to_sum(value);
    }
}
