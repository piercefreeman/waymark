//! Tests that errors from literal and action sub-lowerings propagate intact.

use waymark_vm_ast_old_helpers::{action_stmt, float, function, program, return_stmt};
use waymark_vm_compiler_for_ast_old_test_support::{
    TestLiteralLoweringError, TestLowering, TestSpec,
};

use crate::{CompileError, compile, function::compiler};

use super::{ActionFailingLowering, UnitTestActionLoweringError};

#[test]
fn rejects_non_finite_float_literals() {
    let program = program(vec![function(
        "main",
        &[],
        vec![return_stmt(Some(float(f64::NAN)))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("non-finite literals should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LiteralLowering(
            TestLiteralLoweringError::InvalidFloat(_)
        ))
    ));
}

#[test]
fn preserves_action_lowering_errors() {
    let program = program(vec![function(
        "main",
        &[],
        vec![action_stmt(
            waymark_action_core::ActionRuntime::Python,
            "notify",
        )],
    )]);

    let error = match compile::<TestSpec, ActionFailingLowering>(&program) {
        Ok(_) => panic!("unsupported actions should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::ActionLowering {
            action_name,
            error: UnitTestActionLoweringError::UnsupportedAction,
        }) if action_name == "notify"
    ));
}
