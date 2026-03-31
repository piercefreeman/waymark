use waymark_core_backend::InstanceDone;
use waymark_ids::InstanceId;
use waymark_runner::{DurableUpdates, SleepRequest};
use waymark_worker_core::ActionRequest;

#[derive(Debug)]
pub struct Step {
    pub executor_id: InstanceId,
    pub actions: Vec<ActionRequest>,
    pub sleep_requests: Vec<SleepRequest>,
    pub updates: Option<DurableUpdates>,
    pub instance_done: Option<InstanceDone>,
}
