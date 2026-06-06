//! Core traits and types for VM driver contracts.
//!
//! Split from the driver crate so implementations can depend on
//! the contract without pulling in the full driver machinery.

#![warn(missing_docs)]

mod effect_handler;
mod promise_settler;
mod snapshot_persister;

pub use self::effect_handler::*;
pub use self::promise_settler::*;
pub use self::snapshot_persister::*;
