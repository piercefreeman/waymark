//! Compatibility re-exports for the execution result types that moved to
//! `waymark-worker-core`.  This crate dies with the legacy runner; the
//! re-exports keep the remaining legacy consumers compiling until then.

pub use waymark_worker_core::{
    CheckedExecutionResult, ExecutionException, ExecutionSuccess, UncheckedExecutionResult,
    is_exception_value, uncheck_execution_result,
};
