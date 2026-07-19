//! Backend traits for durably-stored action-call completions.
//!
//! Defines the contract that a database backend must fulfill to support
//! the durable completions flow: completions are recorded as they arrive
//! from the worker pool, polled by demand (the promise ids the VMs are
//! currently waiting on), and removed once their settlements have been
//! durably applied (acked).  Rows of a VM that no longer exists are
//! removed by the store itself alongside the VM (in postgres, a trigger
//! on snapshot deletion), so there is no purge operation here.

#![warn(missing_docs)]

mod common;

pub mod ack_completions;
pub mod poll_completions;
pub mod record_completions;

pub use self::ack_completions::AckCompletions;
pub use self::common::*;
pub use self::poll_completions::PollCompletions;
pub use self::record_completions::RecordCompletions;
