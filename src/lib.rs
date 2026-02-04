//! Rappel - worker pool infrastructure plus the core IR/runtime port.

pub mod messages;
pub mod rappel_core;
pub mod server_worker;
pub mod worker;

// Worker infrastructure (preserved from the legacy Rust core).
pub use messages::{MessageError, ast as ir_ast, proto, workflow_argument_value_to_json};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use worker::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
};
