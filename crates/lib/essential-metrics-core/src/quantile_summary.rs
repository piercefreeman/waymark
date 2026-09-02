//! A distribution held as a quantile sketch.

/// A distribution recorded as a quantile sketch and read at one quantile.
///
/// Nothing selects this shape today; it exists so a metric can, for the
/// case where the ranges of interest are not known in advance. Note what
/// it costs: quantiles do not merge, so a metric shaped this way cannot
/// be rolled up across intervals or across nodes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuantileSummary {
    /// The median of the interval; `None` when it recorded nothing.
    pub p50: Option<f64>,
}
