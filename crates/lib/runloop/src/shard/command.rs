use waymark_core_backend::QueuedInstance;
use waymark_ids::{ExecutionId, InstanceId};
use waymark_worker_core::ActionCompletion;

pub enum Command {
    AssignInstances(Vec<QueuedInstance>),
    ActionCompletions(Vec<ActionCompletion>),
    Wake(Vec<ExecutionId>),
    Evict(Vec<InstanceId>),
    Shutdown,
}
