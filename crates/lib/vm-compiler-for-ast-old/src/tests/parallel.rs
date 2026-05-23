//! Tests for `parallel(...)` aggregation lowering.

use waymark_vm_ast_old::Call;
use waymark_vm_ast_old_helpers::{
    action_call, assignment, assignment_targets, function, parallel_expr, program,
};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::compiler};

#[test]
fn allows_single_target_parallel_expressions_for_aggregation() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment(
            "results",
            parallel_expr(vec![
                Call::Action(action_call("first", Vec::new())),
                Call::Action(action_call("second", Vec::new())),
            ]),
        )],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("single-target parallel expressions should compile");
}

#[test]
fn allows_empty_single_target_parallel_expressions_for_aggregation() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment("results", parallel_expr(Vec::new()))],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("empty single-target parallel expressions should compile");
}

#[test]
fn rejects_parallel_expressions_with_mismatched_targets() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment_targets(
            &["left", "right"],
            parallel_expr(vec![Call::Action(action_call("only", Vec::new()))]),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("mismatched parallel expressions should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::ParallelExprAssignment {
                target_count,
                call_count,
                reason,
                ..
            }
        )) if target_count.get() == 2
            && call_count == 1
            && reason == compiler::UnsupportedParallelExprAssignment::TargetCountMustMatchCalls
    ));
}
