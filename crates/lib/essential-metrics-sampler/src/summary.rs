//! A histogram cell backed by a quantile sketch.
//!
//! Nothing binds a metric to this shape today. It stays because the
//! bucketed alternative needs its boundaries chosen in advance, and a
//! metric whose interesting ranges are not yet known is better served by
//! a sketch — at the price named on
//! [`waymark_essential_metrics_core::QuantileSummary`]: what it produces
//! cannot be aggregated.

/// A histogram cell: observations recorded through the `metrics` handle
/// feed a quantile sketch, drained at sampling time.
#[derive(Debug)]
pub struct Cell(std::sync::Mutex<metrics_util::storage::Summary>);

/// A summary histogram to record the next interval's observations into.
fn summary() -> metrics_util::storage::Summary {
    // The sketch's relative error fixes its dynamic range: bins grow by
    // `(1 + alpha) / (1 - alpha)` apiece, so the widest spread it can
    // hold at once is that ratio raised to the bin count. Past that the
    // store collapses its lowest bins into one, and every quantile
    // falling in the collapsed floor reports that bin instead of a
    // measurement — pinned to `max / range`, and so ratcheting upward
    // with the slowest observation ever seen.
    //
    // `with_defaults` asks for 0.01% error, which leaves a range of only
    // ~702x. One interval's observations can span far more. 1% error is
    // ample for a median, and puts the range beyond anything a spread
    // can reach.
    metrics_util::storage::Summary::new(0.01, 32_768, 1.0e-9)
}

impl Default for Cell {
    fn default() -> Self {
        Self(std::sync::Mutex::new(summary()))
    }
}

impl Cell {
    /// The quantile of the observations recorded since the previous
    /// call, which this call ends: the sketch is replaced, so the next
    /// caller sees a fresh interval and no observation is counted twice.
    ///
    /// Draining is what keeps the quantile a measurement of the present.
    /// A sketch that accumulated for the life of the process would
    /// converge on its own history and stop responding to what the node
    /// is doing now.
    pub fn drain(&self) -> waymark_essential_metrics_core::QuantileSummary {
        let interval = std::mem::replace(&mut *self.0.lock().unwrap(), summary());
        waymark_essential_metrics_core::QuantileSummary {
            p50: interval.quantile(0.5),
        }
    }
}

impl metrics::HistogramFn for Cell {
    fn record(&self, value: f64) {
        self.0.lock().unwrap().add(value);
    }
}
