use uuid::Uuid;
use waymark_core_backend::QueuedInstance;
use waymark_worker_core::ActionCompletion;

pub enum Command {
    AssignInstances(Vec<QueuedInstance>),
    ActionCompletions(Vec<ActionCompletion>),
    Wake(Vec<Uuid>),
    Evict(Vec<Uuid>),
    Shutdown,
}
