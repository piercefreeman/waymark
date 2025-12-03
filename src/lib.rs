//! Rappel - A workflow execution engine with durable Python workers
//!
//! This crate provides the core infrastructure for executing workflow actions
//! in Python worker processes. The key components are:
//!
//! ## Worker Infrastructure
//!
//! - [`WorkerBridgeServer`]: gRPC server that workers connect to
//! - [`PythonWorkerPool`]: Pool of Python worker processes for action execution
//! - [`PythonWorker`]: Individual worker process management
//!
//! ## IR Language
//!
//! - [`lexer`]: Tokenizer for the Rappel IR language with indentation handling
//! - [`parser`]: Recursive descent parser producing proto-based AST
//!
//! ## Database
//!
//! - [`db`]: PostgreSQL database layer with distributed queue (SKIP LOCKED)
//! - [`config`]: Environment-based configuration

pub mod config;
pub mod dag;
pub mod db;
pub mod lexer;
pub mod messages;
pub mod parser;
pub mod server_worker;
pub mod worker;

// Configuration
pub use config::Config;

// Database
pub use db::{
    ActionId, ActionStatus, BackoffKind, CompletionRecord, Database, DbError, DbResult,
    InstanceStatus, NewAction, QueuedAction, RetryKind, WorkflowInstance, WorkflowInstanceId,
    WorkflowVersion, WorkflowVersionId, WorkflowVersionSummary,
};

// Worker infrastructure
pub use messages::{MessageError, proto};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use worker::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
};

// IR language
pub use lexer::{lex, Lexer, LexerError, Span, SpannedToken, Token};
pub use parser::{ast, parse, ParseError, Parser};

// DAG
pub use dag::{convert_to_dag, DAG, DAGConverter, DAGEdge, DAGNode, EdgeType};
