use waymark_ids::{ExecutionId, InstanceId};
use waymark_worker_core::ActionCompletion;

use crate::hydrated_instance::HydratedInstance;

pub enum Command {
    AssignInstances(Vec<HydratedInstance>),
    ActionCompletions(Vec<ActionCompletion>),
    Wake(Vec<ExecutionId>),
    Evict(Vec<InstanceId>),
    Shutdown,
}
