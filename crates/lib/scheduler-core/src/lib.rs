//! Scheduler domain vocabulary and next-run math.
//!
//! These are the app-internal types: the persisted schedule definition
//! blob is this crate's [`ScheduleDefinition`] through the snapshot-plane
//! codec, and the scheduler loop computes run cursors with
//! [`compute_next_run`]. Wire messages terminate at the transport layer;
//! their conversions into these types live in the proto conversion crate,
//! not here.

#![warn(missing_docs)]

mod cron_expression;
mod definition;
mod next_run;
mod status;

pub use self::cron_expression::*;
pub use self::definition::*;
pub use self::next_run::*;
pub use self::status::*;
