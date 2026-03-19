#[derive(Debug, thiserror::Error)]
pub enum PollQueuedInstancesError {
    #[error("sqlx: {0}")]
    Sqlx(sqlx::Error),

    #[error("invalid size: {0}")]
    InvalidSize(std::num::TryFromIntError),

    #[error("database returned empty rows")]
    EmptyRows,

    #[error("decoding queued instance: {0}")]
    QueuedInstanceDecode(crate::codec::DecodeError),

    #[error("decoding graph update: {0}")]
    GraphUpdateDecode(crate::codec::DecodeError),

    #[error("decoding action result: {0}")]
    ActionResultDecode(crate::codec::DecodeError),
}

impl waymark_core_backend::poll_queued_instances::Error for PollQueuedInstancesError {
    fn kind(&self) -> waymark_core_backend::poll_queued_instances::ErrorKind {
        match self {
            PollQueuedInstancesError::EmptyRows => {
                waymark_core_backend::poll_queued_instances::ErrorKind::NoInstances
            }

            PollQueuedInstancesError::Sqlx { .. }
            | PollQueuedInstancesError::InvalidSize { .. }
            | PollQueuedInstancesError::QueuedInstanceDecode { .. }
            | PollQueuedInstancesError::GraphUpdateDecode { .. }
            | PollQueuedInstancesError::ActionResultDecode { .. } => {
                waymark_core_backend::poll_queued_instances::ErrorKind::Internal
            }
        }
    }
}
