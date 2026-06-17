//! Backend traits for the workflow service.
//!
//! Defines the contracts for registering a VM runtime — inserting the
//! initial snapshot and workload pinning rows for a workflow instance.

#![warn(missing_docs)]

/// Shared identifier traits (`HasVmId`, `HasExecutableId`).
pub mod common;
/// VM runtime registration trait and error classification.
pub mod register_vm_runtime;

pub use self::common::{HasExecutableId, HasVmId};
pub use self::register_vm_runtime::RegisterVmRuntime;
