use std::collections::HashSet;

use waymark_core_backend::{ActionDone, GraphUpdate};
use waymark_ids::InstanceId;

pub struct Command {
    pub batch_id: u64,
    pub instance_ids: HashSet<InstanceId>,
    pub graph_instance_ids: HashSet<InstanceId>,
    pub actions_done: Vec<ActionDone>,
    pub graph_updates: Vec<GraphUpdate>,
}
