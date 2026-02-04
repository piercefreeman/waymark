//! Core Rappel runtime modules (ported from core-python).

pub mod backends;
pub mod cli;
pub mod dag;
pub mod dag_viz;
pub mod ir_examples;
pub mod ir_executor;
pub mod ir_format;
pub mod ir_parser;
pub mod runloop;
pub mod runner;
pub mod workers;

pub use backends::{InstanceDone, QueuedInstance};
pub use dag::{
    ActionCallNode, AggregatorNode, AssignmentNode, BranchNode, BreakNode, ContinueNode, DAG,
    DAGConverter, DAGEdge, DAGNode, DagConversionError, EdgeType, ExpressionNode, FnCallNode,
    InputNode, JoinNode, OutputNode, ParallelNode, ReturnNode, convert_to_dag,
};
pub use dag_viz::{build_dag_graph, render_dag_image};
pub use ir_executor::{
    ControlFlow, ExecutionError, ExecutionLimits, FunctionNotFoundError, ParallelExecutionError,
    StatementExecutor, VariableNotFoundError,
};
pub use ir_format::format_program;
pub use runloop::{RunLoop, RunLoopResult};
pub use runner::RunnerState;
pub use workers::{ActionCompletion, ActionRequest, BaseWorkerPool, InlineWorkerPool};
