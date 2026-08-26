//! Backend traits for the scheduler's firing side.
//!
//! Defines the contract a database backend must fulfill for the
//! scheduler loop: polling for due schedules and registering the VM
//! runtimes they spawn. The schedule's definition crosses these traits
//! as an opaque blob — the backend never interprets it; the loop decodes
//! it, computes the advanced run cursor, and hands the backend pure
//! parameters. Claiming happens at registration, fenced on the run
//! cursor, so polling is a plain read and any number of loops may race
//! safely.

#![warn(missing_docs)]

mod common;

pub mod poll_due_schedules;
pub mod register_scheduled_vm_runtimes;

pub use self::common::*;
pub use self::poll_due_schedules::PollDueSchedules;
pub use self::register_scheduled_vm_runtimes::RegisterScheduledVmRuntimes;
