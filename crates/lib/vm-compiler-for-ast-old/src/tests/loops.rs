//! Tests for shared loop-control semantics (`break`/`continue` and `while`).

use waymark_vm_ast_old_helpers::{
    assignment, break_stmt, continue_stmt, function, int, program, return_stmt, variable,
    while_stmt,
};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::compiler};

#[test]
fn rejects_break_outside_loops() {
    let program = program(vec![function("main", &[], vec![break_stmt()])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("break outside a loop should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LoopControlOutsideLoop {
            kind: compiler::LoopControlKind::Break
        })
    ));
}

#[test]
fn rejects_continue_outside_loops() {
    let program = program(vec![function("main", &[], vec![continue_stmt()])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("continue outside a loop should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LoopControlOutsideLoop {
            kind: compiler::LoopControlKind::Continue
        })
    ));
}

#[test]
fn rejects_variables_initialized_only_inside_while_loops() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            while_stmt(variable("flag"), vec![assignment("x", int(1))]),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("loop-only initialization should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "x"
    ));
}
