use std::sync::Arc;

use waymark_core_backend::QueuedInstance;
use waymark_dag::DAG;

#[derive(Debug)]
pub struct HydratedInstance {
    pub instance: QueuedInstance,
    pub dag: Arc<DAG>,
}
