//! Tests for policy brackets on action calls.
//!
//! Policy-annotated action calls route through wrapper-function generation;
//! the brackets themselves are rejected until their lowerings land.

use waymark_vm_ast_old::{
    DurationLiteral, PolicyBracket, RetryPolicy, Spanned, Statement, TimeoutPolicy,
};
use waymark_vm_ast_old_helpers::{action_call, function, program, spanned};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::compiler};

/// Builds a bare action-call statement carrying the provided policies.
fn policy_action_stmt(policies: Vec<PolicyBracket>) -> Spanned<Statement> {
    let mut call = action_call("notify", Vec::new());
    call.policies = policies;
    spanned(Statement::ActionCall { call })
}

#[test]
fn rejects_retry_policies_pending_their_lowering() {
    let program = program(vec![function(
        "main",
        &[],
        vec![policy_action_stmt(vec![PolicyBracket::Retry(
            RetryPolicy {
                exception_types: Vec::new(),
                max_retries: 2,
                backoff: None,
            },
        )])],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("retry policies should fail until their lowering lands"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::RetryPolicy { action_name }
        )) if action_name == "notify"
    ));
}

#[test]
fn rejects_timeout_policies_pending_their_lowering() {
    let program = program(vec![function(
        "main",
        &[],
        vec![policy_action_stmt(vec![PolicyBracket::Timeout(
            TimeoutPolicy {
                timeout: DurationLiteral { seconds: 30 },
            },
        )])],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("timeout policies should fail until their lowering lands"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::TimeoutPolicy { action_name }
        )) if action_name == "notify"
    ));
}
