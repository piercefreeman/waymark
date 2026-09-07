//! The VM driver's events: one per observation of a run, summarized.
//!
//! A run is observed through the driver's hooks; the observability side
//! implements them and turns each observation into the payload here —
//! names, classifications and sizes, never the values themselves.

mod kind;
mod payload;

pub use kind::*;
pub use payload::*;
