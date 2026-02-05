//! Rappel - worker pool infrastructure plus the core IR/runtime port.

pub mod backends;
pub mod config;
pub mod db;
pub mod messages;
pub mod observability;
pub mod pool_status;
pub mod rappel_core;
pub mod scheduler;
pub mod server_worker;
pub mod webapp;
pub mod workers;

// Worker infrastructure (preserved from the legacy Rust core).
pub use messages::{MessageError, ast as ir_ast, proto, workflow_argument_value_to_json};
pub use observability::obs;
pub use pool_status::{PoolTimeSeries, TimeSeriesEntry, TimeSeriesJsonEntry};
pub use scheduler::{
    CreateScheduleParams, ScheduleId, ScheduleType, SchedulerConfig, SchedulerTask,
    WorkflowSchedule, spawn_scheduler,
};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use webapp::{WebappConfig, WebappServer};
pub use workers::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RemoteWorkerPool,
    RoundTripMetrics, spawn_status_reporter,
};
