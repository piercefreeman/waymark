//! Waymark - worker pool infrastructure plus the core IR/runtime port.

pub mod config;
pub mod garbage_collector;
pub mod messages;
pub mod pool_status;
pub mod scheduler;
pub mod server_worker;
pub mod waymark_core;
pub mod webapp;
pub mod workers;

// Worker infrastructure (preserved from the legacy Rust core).
pub use garbage_collector::{GarbageCollectorConfig, GarbageCollectorTask};
pub use messages::MessageError;
pub use pool_status::{PoolTimeSeries, TimeSeriesEntry, TimeSeriesJsonEntry};
pub use scheduler::{SchedulerConfig, SchedulerTask};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use webapp::{WebappConfig, WebappServer};
pub use workers::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RemoteWorkerPool,
    RoundTripMetrics, spawn_status_reporter,
};
