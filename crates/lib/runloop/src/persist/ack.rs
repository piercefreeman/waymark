use waymark_core_backend::InstanceLockStatus;

pub enum Ack {
    StepsPersisted {
        batch_id: u64,
        lock_statuses: Vec<InstanceLockStatus>,
    },
    StepsPersistFailed {
        batch_id: u64,
        error: String, /* TODO: this needs a proper type, but not `RunLoopError` */
    },
}
