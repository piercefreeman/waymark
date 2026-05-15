use waymark_vm_ast_old_helpers::{function, program};

use super::super::table::FunctionTable;

pub(crate) use waymark_vm_compiler_for_ast_old_test_support::{
    TestActionRef, TestConstValue, TestLowering, TestSpec,
};

pub(crate) fn build_function_table() -> FunctionTable {
    let program = program(vec![function("child", &["value"], vec![])]);
    FunctionTable::build(&program).expect("function table should build")
}
