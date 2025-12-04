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

pub mod ast_evaluator;
pub mod ast_printer;
pub mod config;
pub mod dag;
pub mod dag_state;
pub mod db;
pub mod ir_printer;
pub mod lexer;
pub mod messages;
pub mod parser;
pub mod runner;
pub mod server_client;
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
pub use messages::{MessageError, ast as ir_ast, proto};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use worker::{
    ActionDispatchPayload, PythonWorker, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
};

// IR language
pub use lexer::{Lexer, LexerError, Span, SpannedToken, Token, lex};
pub use parser::{ParseError, Parser, ast, parse};

// DAG
pub use dag::{DAG, DAGConverter, DAGEdge, DAGNode, EdgeType, convert_to_dag};

// AST Printer
pub use ast_printer::{AstPrinter, print_expr, print_program, print_statement};

// DAG State Helper
pub use dag_state::{DAGHelper, DataFlowTarget, ExecutionMode, SuccessorInfo};

// AST Evaluator
pub use ast_evaluator::{EvaluationError, EvaluationResult, ExpressionEvaluator, Scope};

// Runner
pub use runner::{
    CompletionBatch, DAGRunner, InFlightTracker, InboxWrite, RunnerConfig, RunnerError,
    RunnerResult, WorkCompletionHandler, WorkQueueHandler, WorkerSlotTracker,
};
