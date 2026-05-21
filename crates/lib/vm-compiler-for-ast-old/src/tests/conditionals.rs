//! Tests for `if`/`elif`/`else` flow-state handling.

use waymark_vm_ast_old_helpers::{
    assignment, conditional_stmt, function, int, program, return_stmt, variable,
};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::compiler};

#[test]
fn rejects_variables_missing_on_some_conditional_paths() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                Vec::new(),
                None,
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("partially initialized conditionals should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "x"
    ));
}

#[test]
fn allows_variables_initialized_on_all_conditional_paths() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                Vec::new(),
                Some(vec![assignment("x", int(2))]),
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("variables initialized on every path should compile");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");

    assert_eq!(function.num_regs, 2);
}

#[test]
fn allows_variables_initialized_on_all_if_elif_and_else_paths() {
    let program = program(vec![function(
        "main",
        &["flag", "fallback"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                vec![(variable("fallback"), vec![assignment("x", int(2))])],
                Some(vec![assignment("x", int(3))]),
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("variables initialized on every if/elif/else path should compile");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");

    assert_eq!(function.num_regs, 3);
}

#[test]
fn rejects_variables_missing_on_an_elif_path() {
    let program = program(vec![function(
        "main",
        &["flag", "fallback"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                vec![(variable("fallback"), Vec::new())],
                Some(vec![assignment("x", int(3))]),
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("missing elif initialization should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "x"
    ));
}

#[test]
fn reuses_function_scope_locals_after_branch_only_declarations() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                Vec::new(),
                None,
            ),
            assignment("x", int(2)),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("locals declared in a branch should stay bound at function scope");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");

    assert_eq!(function.num_regs, 2);
}
