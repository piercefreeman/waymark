//! Tests for variable resolution and copy-assignment lowering.

use waymark_vm_ast_old_helpers::{assignment, function, int, program, return_stmt, variable};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};
use waymark_vm_instructions_fullset::FullSet;
use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_runtime_core::RegisterId;

use crate::{CompileError, compile, function::compiler};

#[test]
fn rejects_unknown_variables() {
    let program = program(vec![function(
        "main",
        &[],
        vec![return_stmt(Some(variable("missing")))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("unknown variables should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "missing"
    ));
}

#[test]
fn rejects_self_referential_assignments_to_new_variables() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment("value", variable("value"))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("self-referential assignment should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "value"
    ));
}

#[test]
fn lowers_copy_assignments_with_a_copy_instruction() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("x", int(1)),
            assignment("y", variable("x")),
            return_stmt(Some(variable("y"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("copy assignment should lower successfully");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");
    let state = function
        .states
        .iter()
        .next()
        .expect("entry state should exist");

    assert!(state.instructions.iter().any(|instruction| {
        matches!(
            instruction,
            FullSet::PureSet(PureSet::Copy {
                dst: RegisterId(1),
                src: RegisterId(0),
            })
        )
    }));
}
