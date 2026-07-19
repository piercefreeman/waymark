//! Backend traits for durably-recorded sleep requests.
//!
//! Defines the contract that a database backend must fulfill to support
//! the durable sleep flow: sleeps are recorded as the VM emits them,
//! polled by demand (the promise ids the VMs are currently waiting on)
//! once their deadlines pass, and removed once their settlements have
//! been durably applied (acked).  The ack itself deletes the recorded
//! request, so a single table backs the whole flow.  Rows of a VM that
//! no longer exists are removed by the store itself alongside the VM
//! (in postgres, a trigger on snapshot deletion), so there is no purge
//! operation here.

#![warn(missing_docs)]

mod common;

pub mod ack_sleeps;
pub mod poll_due_sleeps;
pub mod record_sleeps;

pub use self::ack_sleeps::AckSleeps;
pub use self::common::*;
pub use self::poll_due_sleeps::PollDueSleeps;
pub use self::record_sleeps::RecordSleeps;
