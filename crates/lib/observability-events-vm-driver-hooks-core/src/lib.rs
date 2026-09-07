//! Summarizing the VM driver's effects for its observability events.

#![warn(missing_docs)]

/// Summarizes one interpreter's effects: what identifies an effect,
/// without the values it carries.
///
/// A summarizer is a type, not a value — the crate that knows an effect
/// type defines one and implements this for it.
pub trait SummarizeEffect {
    /// The effect summarized.
    type Effect;

    /// The summary produced.
    type Summary;

    /// Summarize `effect`.
    fn summarize_effect(effect: &Self::Effect) -> Self::Summary;
}
