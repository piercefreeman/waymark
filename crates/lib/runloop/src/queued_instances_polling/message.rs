use nonempty_collections::NEVec;
use waymark_core_backend::QueuedInstance;

#[derive(Debug)]
pub enum Message<BackendError> {
    Batch {
        instances: NEVec<QueuedInstance>,
        reserve: crate::available_instance_slots::NonZeroReserve,
    },
    Pending,
    Error(BackendError),
}
