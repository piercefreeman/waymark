use uuid::Uuid;

use crate::shard;

pub enum Event {
    Step(shard::Step),
    InstanceFailed {
        executor_id: Uuid,
        entry_node: Uuid,
        error: String,
    },
}
