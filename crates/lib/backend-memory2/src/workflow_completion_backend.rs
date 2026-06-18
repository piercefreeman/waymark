//! In-memory backend for workflow completion persistence.

use waymark_ids::InstanceId;
use waymark_workflow_completion_backend::Outcome;

// ---------------------------------------------------------------------------
// HasVmId
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::HasVmId for crate::MemoryBackend {
    type VmId = InstanceId;
}

// ---------------------------------------------------------------------------
// RecordCompletion
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::RecordCompletion for crate::MemoryBackend {
    type Error = RecordError;

    async fn record_completion<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        value: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        self.record(Outcome::Completion(value.as_ref().to_vec()), *vm_id)
    }
}

// ---------------------------------------------------------------------------
// RecordException
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::RecordException for crate::MemoryBackend {
    type Error = RecordError;

    async fn record_exception<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        exception: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        self.record(Outcome::Exception(exception.as_ref().to_vec()), *vm_id)
    }
}

impl crate::MemoryBackend {
    fn record(&self, outcome: Outcome, vm_id: InstanceId) -> Result<(), RecordError> {
        let mut guard = self.vm_execution_results.lock().unwrap();
        match guard.get(&vm_id) {
            Some(existing) if *existing != outcome => Err(RecordError::Conflict(vm_id)),
            _ => {
                guard.insert(vm_id, outcome);
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PollOutcome
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::PollOutcome for crate::MemoryBackend {
    type Error = std::convert::Infallible;

    async fn poll_outcome<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> Result<Option<Outcome>, Self::Error> {
        let guard = self.vm_execution_results.lock().unwrap();
        Ok(guard.get(vm_id).cloned())
    }
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("conflicting outcome already recorded for vm {0}")]
    Conflict(InstanceId),
}
