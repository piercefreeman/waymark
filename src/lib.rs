//! Rappel - A workflow execution engine with durable Python workers
//!
//! This crate provides the core infrastructure for executing workflow actions
//! in Python worker processes. The key components are:
//!
//! - [`WorkerBridgeServer`]: gRPC server that workers connect to
//! - [`PythonWorkerPool`]: Pool of Python worker processes for action execution
//! - [`PythonWorker`]: Individual worker process management

pub mod messages;
pub mod server_worker;
pub mod worker;

pub use messages::{MessageError, proto};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use worker::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
};
