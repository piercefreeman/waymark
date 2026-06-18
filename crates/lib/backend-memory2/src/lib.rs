//! In-memory backend for the new persistence traits.
//!
//! Implements [`waymark_workflow_completion_backend`],
//! [`waymark_workflow_service_vm_runtimes_backend`],
//! [`waymark_workload_pinning_backend`], and
//! [`waymark_state_vm_runtimes_backend`] in memory.

mod state_vm_executables_backend;
mod state_vm_runtimes_backend;
mod workflow_completion_backend;
mod workflow_service_vm_executables_backend;
mod workflow_service_vm_runtimes_backend;
mod workload_pinning_backend;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use waymark_ids::{InstanceId, WorkflowVersionId};

type Table<T> = Arc<Mutex<T>>;

pub(crate) type VmExecutionResultsStore =
    HashMap<InstanceId, waymark_workflow_completion_backend::Outcome>;

#[derive(Clone, Debug)]
pub(crate) struct SnapshotEntry {
    pub(crate) executable_id: WorkflowVersionId,
    pub(crate) snapshot: Vec<u8>,
}

pub(crate) type VmRuntimeSnapshotStore = HashMap<InstanceId, SnapshotEntry>;

#[derive(Clone, Debug)]
pub(crate) struct PinningEntry {
    pub(crate) node_id: uuid::Uuid,
    pub(crate) expires_at: DateTime<Utc>,
}

pub(crate) type WorkloadPinningStore = HashMap<InstanceId, Option<PinningEntry>>;

pub(crate) type ExecutableNameVersionKey = (String, String);
pub(crate) type ExecutablesStore = HashMap<ExecutableNameVersionKey, (WorkflowVersionId, Vec<u8>)>;

pub(crate) type ExecutableByIdStore = HashMap<WorkflowVersionId, Vec<u8>>;

/// Backend that stores new-trait updates in memory for tests or local runs.
#[derive(Default)]
pub struct MemoryBackend {
    vm_execution_results: Table<VmExecutionResultsStore>,
    vm_runtime_snapshots: Table<VmRuntimeSnapshotStore>,
    workload_pinnings: Table<WorkloadPinningStore>,
    executables: Table<ExecutablesStore>,
    executables_by_id: Table<ExecutableByIdStore>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}
