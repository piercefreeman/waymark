//! Tests for function-table construction and duplicate-definition rejection.

use waymark_vm_ast_old_helpers::{function, int, program, return_stmt};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

use crate::{CompileError, compile, function::table};

#[test]
fn rejects_duplicate_function_names() {
    let program = program(vec![
        function("main", &[], vec![return_stmt(Some(int(1)))]),
        function("main", &[], vec![return_stmt(Some(int(2)))]),
    ]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("duplicate function names should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionTable(table::Error::DuplicateFunction { name }) if name == "main"
    ));
}

#[test]
fn rejects_duplicate_inputs() {
    let program = program(vec![function("main", &["value", "value"], vec![])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("duplicate inputs should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(crate::function::compiler::Error::DuplicateInput {
            function,
            name,
        }) if function == "main" && name == "value"
    ));
}
