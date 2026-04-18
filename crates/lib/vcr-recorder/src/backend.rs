use waymark_workflow_registry_backend::WorkflowVersion;

use crate::{Command, HandleError};

#[derive(Debug, Clone)]
pub struct Handle {
    tx: tokio::sync::mpsc::Sender<crate::Command>,
}

impl Handle {
    pub fn handle_seen_instance(
        &self,
        queued_instance: waymark_core_backend::QueuedInstance,
    ) -> Result<(), HandleError> {
        self.tx
            .try_send(Command::OpenInstanceLog(queued_instance))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => HandleError::RecorderDropped,
                tokio::sync::mpsc::error::TrySendError::Full(_) => HandleError::NoBufferCapacity,
            })
    }

    pub fn handle_seen_instance_done(
        &self,
        instance_id: waymark_ids::InstanceId,
    ) -> Result<(), HandleError> {
        self.tx
            .try_send(Command::CompleteInstanceLog(instance_id))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => HandleError::RecorderDropped,
                tokio::sync::mpsc::error::TrySendError::Full(_) => HandleError::NoBufferCapacity,
            })
    }

    pub fn handle_seen_workflow_version(
        &self,
        workflow_version: WorkflowVersion,
    ) -> Result<(), HandleError> {
        self.tx
            .try_send(Command::RecordWorkflowVersion(workflow_version))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => HandleError::RecorderDropped,
                tokio::sync::mpsc::error::TrySendError::Full(_) => HandleError::NoBufferCapacity,
            })
    }
}

impl crate::Handle {
    pub fn backend_handle(&self) -> Handle {
        Handle {
            tx: self.tx.clone(),
        }
    }
}
