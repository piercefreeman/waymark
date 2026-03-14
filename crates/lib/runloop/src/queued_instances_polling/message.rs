use waymark_backends_core::BackendError;
use waymark_core_backend::QueuedInstance;

pub enum Message {
    Batch { instances: Vec<QueuedInstance> },
    Error(BackendError),
}
