use waymark_core_backend::InstanceLockStatus;

use crate::RunLoopError;

pub enum Ack {
    StepsPersisted {
        batch_id: u64,
        lock_statuses: Vec<InstanceLockStatus>,
    },
    StepsPersistFailed {
        batch_id: u64,
        error: RunLoopError,
    },
}
