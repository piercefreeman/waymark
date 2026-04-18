use crate::{Command, HandleError};

#[derive(Debug, Clone)]
pub struct Handle {
    tx: tokio::sync::mpsc::Sender<crate::Command>,
}

impl Handle {
    pub fn request(&self, request: waymark_worker_core::ActionRequest) -> Result<(), HandleError> {
        self.tx
            .try_send(Command::RecordActionRequest(request))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => HandleError::RecorderDropped,
                tokio::sync::mpsc::error::TrySendError::Full(_) => HandleError::NoBufferCapacity,
            })
    }

    pub fn completion(
        &self,
        completion: waymark_worker_core::ActionCompletion,
    ) -> Result<(), HandleError> {
        self.tx
            .try_send(Command::RecordActionCompletion(completion))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => HandleError::RecorderDropped,
                tokio::sync::mpsc::error::TrySendError::Full(_) => HandleError::NoBufferCapacity,
            })
    }
}

impl crate::Handle {
    pub fn pool_handle(&self) -> Handle {
        Handle {
            tx: self.tx.clone(),
        }
    }
}
