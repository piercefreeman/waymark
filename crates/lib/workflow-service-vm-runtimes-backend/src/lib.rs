//! Backend traits for the workflow service.
//!
//! Defines the contracts for registering a VM runtime — inserting the
//! initial snapshot and workload pinning rows for a workflow instance.

#![warn(missing_docs)]

/// Shared identifier traits (`HasVmId`, `HasExecutableId`).
pub mod common;
/// Batch existence query for registered VM runtimes.
pub mod find_existing_vm_runtimes;
/// VM runtime registration trait.
pub mod register_vm_runtimes;

pub use self::common::{HasExecutableId, HasVmId};
pub use self::find_existing_vm_runtimes::FindExistingVmRuntimes;
pub use self::register_vm_runtimes::RegisterVmRuntimes;
