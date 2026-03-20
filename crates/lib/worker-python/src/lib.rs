//! Remote worker process management.
//!
//! This module provides the core infrastructure for spawning and managing
//! Python worker processes that execute workflow actions.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                           PythonWorkerPool                               │
//! │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                        │
//! │  │PythonWorker │ │PythonWorker │ │PythonWorker │  ... (N workers)       │
//! │  │  (process)  │ │  (process)  │ │  (process)  │                        │
//! │  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘                        │
//! │         │               │               │                                │
//! │         └───────────────┼───────────────┘                                │
//! │                         │ gRPC streaming                                 │
//! │                         ▼                                                │
//! │              ┌─────────────────────┐                                     │
//! │              │  WorkerBridgeServer │                                     │
//! │              └─────────────────────┘                                     │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Worker Lifecycle
//!
//! 1. Pool spawns N worker processes, each connecting to the WorkerBridge
//! 2. Workers send `WorkerHello` to complete the handshake
//! 3. Pool sends `ActionDispatch` messages, workers respond with `ActionResult`
//! 4. Workers send `Ack` immediately upon receiving a dispatch (for latency tracking)
//! 5. On shutdown, workers are terminated gracefully
//!
//! ## Error Handling
//!
//! - Worker spawn failures are propagated immediately
//! - Connection timeouts (15s) trigger worker process termination
//! - Dropped channels indicate worker crash and are propagated as errors
//! - Round-robin selection ensures load distribution even with slow workers

#[cfg(test)]
mod tests;

pub mod config;
pub mod pool;
pub mod process;

mod shared_state;
mod utils;

pub use self::config::Config;
pub use self::pool::Pool;
pub use self::process::Process;
