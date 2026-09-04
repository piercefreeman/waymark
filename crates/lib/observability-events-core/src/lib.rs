//! Core types of the observability-events subsystem: the event record, and
//! what every payload type tells the readers about itself.
//!
//! The crate's surface is flat — callers name
//! `waymark_observability_events_core::Event` rather than routing through
//! the module that happens to define it — except for [`kind`], which
//! groups the traits of a kind under its own path, the way the query
//! backend's `list_events` and `tail` group a read's; [`Kinded`], what every
//! payload type carries, stays at the root.

#![warn(missing_docs)]

mod event;
pub mod kind;

pub use event::*;
pub use kind::Kinded;
