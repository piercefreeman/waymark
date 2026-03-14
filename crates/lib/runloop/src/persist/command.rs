use std::collections::HashSet;

use uuid::Uuid;
use waymark_core_backend::{ActionDone, GraphUpdate};

pub struct Command {
    pub batch_id: u64,
    pub instance_ids: HashSet<Uuid>,
    pub graph_instance_ids: HashSet<Uuid>,
    pub actions_done: Vec<ActionDone>,
    pub graph_updates: Vec<GraphUpdate>,
}
