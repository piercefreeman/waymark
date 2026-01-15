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
pub mod completion;
pub mod config;
pub mod dag;
pub mod dag_state;
pub mod db;
pub mod ir_printer;
pub mod ir_validation;
pub mod lexer;
pub mod messages;
pub mod parser;
pub mod runner;
pub mod schedule;
pub mod server_client;
pub mod server_webapp;
pub mod server_worker;
pub mod traversal;
pub mod value;
pub mod worker;

// Configuration
pub use config::{
    Config, DEFAULT_BASE_PORT, DEFAULT_WEBAPP_ADDR, WebappConfig, get_config, try_get_config,
};

// Database
pub use db::{
    ActionId, ActionStatus, BackoffKind, CompletionRecord, Database, DbError, DbResult,
    InstanceStatus, NewAction, QueuedAction, RetryKind, ScheduleId, ScheduleStatus, ScheduleType,
    WorkflowInstance, WorkflowInstanceId, WorkflowSchedule, WorkflowVersion, WorkflowVersionId,
    WorkflowVersionSummary,
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

// Runner
pub use runner::{
    CompletionBatch, DAGRunner, InFlightTracker, InboxWrite, RunnerConfig, RunnerError,
    RunnerMetricsSnapshot, RunnerResult, WorkCompletionHandler, WorkQueueHandler,
    WorkerSlotTracker,
};

// Completion (unified readiness model)
pub use completion::{
    CompletionError, CompletionPlan, CompletionResult, FrontierCategory, FrontierNode, GuardResult,
    InlineScope, InstanceCompletion, NodeType, ReadinessIncrement, SubgraphAnalysis,
    analyze_subgraph, evaluate_guard, execute_inline_subgraph, find_direct_predecessor_in_path,
    is_direct_predecessor,
};

// Webapp server
pub use server_webapp::WebappServer;

// Schedule utilities
pub use schedule::{next_cron_run, next_interval_run, validate_cron};

// Traversal (shared DAG traversal logic)
pub use traversal::{
    InlineScope as TraversalScope, LoopAwareTraversal, MAX_LOOP_ITERATIONS, TraversalEdge,
    TraversalQueue, WorkQueueEntry, evaluate_guard as traversal_evaluate_guard,
    get_traversal_successors, select_guarded_edges,
};
