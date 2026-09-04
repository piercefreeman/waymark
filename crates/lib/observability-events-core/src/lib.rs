//! Core types of the observability-events family: the event record, and
//! what every payload type tells the readers about itself.
//!
//! The modules are for arranging the source; the crate's surface is flat,
//! so callers name `waymark_observability_events_core::Event` rather than
//! routing through the module that happens to define it.

#![warn(missing_docs)]

mod event;
mod kind;

pub use event::*;
pub use kind::*;
