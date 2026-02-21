//! Waymark - worker pool infrastructure plus the core IR/runtime port.

pub mod backends;
pub mod config;
pub mod db;
pub mod garbage_collector;
pub mod integration_support;
pub mod messages;
pub mod observability;
pub mod pool_status;
pub mod scheduler;
pub mod server_worker;
#[cfg(test)]
pub mod test_support;
pub mod waymark_core;
pub mod webapp;
pub mod workers;

// Worker infrastructure (preserved from the legacy Rust core).
pub use garbage_collector::{GarbageCollectorConfig, GarbageCollectorTask};
pub use messages::{MessageError, ast as ir_ast, proto, workflow_argument_value_to_json};
pub use observability::obs;
pub use pool_status::{PoolTimeSeries, TimeSeriesEntry, TimeSeriesJsonEntry};
pub use scheduler::{
    CreateScheduleParams, ScheduleId, ScheduleType, SchedulerConfig, SchedulerTask,
    WorkflowSchedule,
};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use webapp::{WebappConfig, WebappServer};
pub use workers::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RemoteWorkerPool,
    RoundTripMetrics, spawn_status_reporter,
};
