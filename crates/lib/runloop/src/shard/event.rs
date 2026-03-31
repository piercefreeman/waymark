use waymark_ids::{ExecutionId, InstanceId};

use crate::shard;

pub enum Event {
    Step(shard::Step),
    InstanceFailed {
        executor_id: InstanceId,
        entry_node: ExecutionId,
        error: String,
    },
}
