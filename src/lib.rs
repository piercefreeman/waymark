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
pub mod execution_events;
pub mod execution_graph;
pub mod executor;
pub mod ir_printer;
pub mod ir_validation;
pub mod lexer;
pub mod messages;
pub mod parser;
pub mod pool_status;
pub mod runner_database;
pub mod runner_memory;
pub mod schedule;
pub mod server_client;
pub mod server_webapp;
pub mod server_worker;
pub mod stats;
pub mod value;
pub mod worker;
mod workflow_ir;

// Configuration
pub use config::{
    Config, DEFAULT_BASE_PORT, DEFAULT_WEBAPP_ADDR, WebappConfig, get_config, try_get_config,
};

// Database
pub use db::{
    ClaimedInstance, Database, DbError, DbResult, InstanceStatus, ScheduleId, ScheduleStatus,
    ScheduleType, WorkflowInstance, WorkflowInstanceId, WorkflowSchedule, WorkflowVersion,
    WorkflowVersionId, WorkflowVersionSummary,
};

// Worker infrastructure
pub use messages::{MessageError, ast as ir_ast, proto, workflow_arguments_to_json};
pub use server_worker::{WorkerBridgeChannels, WorkerBridgeServer};
pub use value::WorkflowValue;
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
pub use ir_validation::validate_program;

// DAG State Helper
pub use dag_state::{DAGHelper, DataFlowTarget, ExecutionMode, SuccessorInfo};

// AST Evaluator
pub use ast_evaluator::{EvaluationError, EvaluationResult, ExpressionEvaluator, Scope};

// Webapp server
pub use server_webapp::WebappServer;

// Lifecycle statistics
pub use stats::{LifecycleStats, LifecycleStatsSnapshot, MetricStats};

// Schedule utilities
pub use schedule::{next_cron_run, next_interval_run, validate_cron};

// Execution Graph (instance-local execution model)
pub use execution_graph::{BatchCompletionResult, Completion, ExecutionState, MAX_LOOP_ITERATIONS};

// Execution Events (append-only log model)
pub use execution_events::{ApplyEventError, ExecutionEvent, ExecutionStateMachine, apply_event};

// Instance Runner (lease-based execution)
pub use runner_database::{
    DEFAULT_CLAIM_BATCH_SIZE, DEFAULT_COMPLETION_BATCH_SIZE, DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_LEASE_SECONDS, DEFAULT_SCHEDULE_CHECK_BATCH_SIZE, DEFAULT_SCHEDULE_CHECK_INTERVAL,
    InstanceRunner, InstanceRunnerConfig, InstanceRunnerError, InstanceRunnerMetrics,
    InstanceRunnerResult,
};
