use nonempty_collections::NEVec;
use waymark_core_backend::QueuedInstance;

pub enum Message<BackendError> {
    Batch { instances: NEVec<QueuedInstance> },
    Pending,
    Error(BackendError),
}
