//! A per-second rate derived from a sampled cumulative counter.

#![warn(missing_docs)]

mod instant;
mod total;

pub use self::instant::*;
pub use self::total::*;

/// Derives a per-second rate from a cumulative counter sampled at
/// instants: each reading that advances past the previous distinct one
/// yields the window between them, measured over their timestamp
/// spread. Re-observing the same reading yields nothing, so an observer
/// polling faster than the counter is sampled keeps the last yielded
/// window itself if it wants a latest-known value.
#[derive(Debug)]
pub struct CounterRate<Instant, Total> {
    baseline: Option<Baseline<Instant, Total>>,
}

/// The last distinct reading the next window measures from.
#[derive(Debug)]
struct Baseline<Instant, Total> {
    at: Instant,
    total: Total,
}

/// The outcome of one observed reading.
#[derive(Debug)]
pub enum Observation<Total> {
    /// The first reading: it becomes the baseline; there is nothing to
    /// measure against yet.
    First,

    /// The reading advanced past the baseline: the window between them,
    /// and the reading is the new baseline.
    Advanced(Window<Total>),

    /// The reading is the baseline again — the counter simply has not
    /// been sampled anew; nothing changes.
    Unchanged,
}

/// Error observing a reading; the baseline stays as it was.
#[derive(Debug, thiserror::Error)]
pub enum ObserveError {
    /// The reading is older than the baseline — the readings arrived
    /// out of order.
    #[error("the reading is older than the baseline")]
    Regressed,

    /// The reading's instant does not compare to the baseline's.
    #[error("the reading's instant does not compare to the baseline's")]
    Incomparable,
}

/// One derived measurement window.
#[derive(Debug)]
pub struct Window<Total> {
    /// The counter's growth within the window. The counter resetting
    /// (e.g. across a process restart) saturates the delta to zero,
    /// which reads as a (real) progress gap.
    pub delta: Total,

    /// The delta over the window's timestamp spread.
    pub per_second: f64,
}

impl<Instant, Total> CounterRate<Instant, Total>
where
    Instant: crate::Instant,
    Total: crate::Total,
{
    /// A tracker with nothing observed yet.
    #[expect(
        clippy::new_without_default,
        reason = "an empty tracker is a deliberate construction, not a default value"
    )]
    pub const fn new() -> Self {
        Self { baseline: None }
    }

    /// Observe one timestamped counter reading; only a reading provably
    /// newer than the baseline derives a window and moves the baseline.
    pub fn observe(
        &mut self,
        at: Instant,
        total: Total,
    ) -> Result<Observation<Total>, ObserveError> {
        let Some(baseline) = &self.baseline else {
            self.baseline = Some(Baseline { at, total });
            return Ok(Observation::First);
        };

        match at.partial_cmp(&baseline.at) {
            Some(std::cmp::Ordering::Greater) => {}
            Some(std::cmp::Ordering::Equal) => return Ok(Observation::Unchanged),
            Some(std::cmp::Ordering::Less) => return Err(ObserveError::Regressed),
            None => return Err(ObserveError::Incomparable),
        }

        let window_secs = at.seconds_since(baseline.at);
        let delta = total.delta_since(baseline.total);
        self.baseline = Some(Baseline { at, total });

        Ok(Observation::Advanced(Window {
            delta,
            per_second: delta.as_f64() / window_secs.max(f64::EPSILON),
        }))
    }
}
