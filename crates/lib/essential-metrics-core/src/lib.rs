//! Core types of the essential-metrics subsystem: the typed node sample
//! and the shapes its distributions take.
//!
//! The modules are for arranging the source; the crate's surface is flat,
//! so callers name `waymark_essential_metrics_core::NodeSample` rather
//! than routing through the module that happens to define it.

#![warn(missing_docs)]

mod bounds;
mod bucketed_histogram;
mod node_sample;
mod quantile_summary;

pub use bounds::*;
pub use bucketed_histogram::*;
pub use node_sample::*;
pub use quantile_summary::*;
