//! Backend traits for reading the observability state.
//!
//! The state is derived from the observability events by whoever
//! implements these; a read's cursor is the backend's own type, as for
//! the events reads.

#![warn(missing_docs)]

mod common;
pub mod get_instance;
pub mod list_instances;

pub use self::common::*;

pub use self::get_instance::GetInstance;
pub use self::list_instances::ListInstances;
