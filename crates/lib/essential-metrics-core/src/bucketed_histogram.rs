//! A distribution held as counts over fixed bucket boundaries.

/// A distribution recorded as counts over fixed bucket boundaries.
///
/// There is one more count than there are bounds: `counts[i]` holds the
/// observations less than or equal to bound `i`, and the final count —
/// which no bound corresponds to — holds every observation, including
/// those above the last bound. So the counts rise across the array and
/// the last of them is the total.
///
/// Cumulative because that is what a quantile reads directly, and
/// because counts in this form sum elementwise: merging two nodes'
/// histograms, or two sampling intervals', is adding them. That is the
/// property a quantile itself does not have, and the reason these
/// replaced a median on the wire.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketedHistogram<const N: usize> {
    /// Observations at or below each corresponding bound, then the
    /// total.
    pub counts: [u64; N],

    /// The observed values added together, for the mean the buckets
    /// cannot give back.
    pub sum: f64,
}

impl<const N: usize> BucketedHistogram<N> {
    /// The value at `quantile`, interpolated linearly within the bucket
    /// it falls in, against the `bounds` these counts were recorded
    /// against.
    ///
    /// `None` when nothing was observed, and `None` when the quantile
    /// falls above the last bound — that bucket has no upper edge to
    /// interpolate towards, and naming its lower edge would report a
    /// number the data does not support.
    pub fn quantile(&self, bounds: &[f64], quantile: Quantile) -> Option<f64> {
        let total = *self.counts.last()?;
        if total == 0 {
            return None;
        }
        let target = total as f64 * quantile.get();
        let bucket = self
            .counts
            .iter()
            .position(|count| *count as f64 >= target)?;
        let upper_bound = *bounds.get(bucket)?;
        let lower_bound = if bucket == 0 { 0.0 } else { bounds[bucket - 1] };
        let lower_count = if bucket == 0 {
            0
        } else {
            self.counts[bucket - 1]
        };
        // The bucket holds the target, which is above zero, so it holds at
        // least one observation above the previous count.
        let within = self.counts[bucket] - lower_count;
        let fraction = (target - lower_count as f64) / within as f64;
        Some(lower_bound + (upper_bound - lower_bound) * fraction)
    }
}

/// A fraction of a distribution, in `(0, 1]`: the share of the
/// observations at or below the value a quantile names.
///
/// Zero is excluded because it names no observation, and nothing above one
/// exists; a fraction outside the range has no value to read.
#[derive(Debug)]
pub struct Quantile(f64);

impl Quantile {
    /// The median.
    pub const MEDIAN: Self = Self(0.5);

    /// A fraction in `(0, 1]`; `None` for anything else, `NaN` included.
    pub fn new(fraction: f64) -> Option<Self> {
        (fraction > 0.0 && fraction <= 1.0).then_some(Self(fraction))
    }

    /// The fraction as a number.
    pub fn get(&self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests;
