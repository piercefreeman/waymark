mod actions_done_persistence;
mod execution_state_persistence;
mod instance_enqueue;
mod instance_queue_completion;
mod instance_queue_locks_keepalive;
mod instance_queue_locks_release;
mod instance_queue_poll;

pub use self::actions_done_persistence::*;
pub use self::execution_state_persistence::*;
pub use self::instance_enqueue::*;
pub use self::instance_queue_completion::*;
pub use self::instance_queue_locks_keepalive::*;
pub use self::instance_queue_locks_release::*;
pub use self::instance_queue_poll::*;

/// All core backend traits.
pub trait Core:
    InstanceEnqueue
    + InstanceQueuePoll
    + InstanceQueueCompletion
    + InstanceQueueLocksKeepalive
    + InstanceQueueLocksRelease
    + ExecutionStatePersistence
    + ActionsDonePersistence
{
}

impl<T> Core for T where
    T: InstanceEnqueue
        + InstanceQueuePoll
        + InstanceQueueCompletion
        + InstanceQueueLocksKeepalive
        + InstanceQueueLocksRelease
        + ExecutionStatePersistence
        + ActionsDonePersistence
{
}
