use waymark_vm_compiler_for_ast_old_test_support::{
    TestExecutable, TestLowering, TestReadyValue, TestSpec, TestValue,
};

type TestInterpreter =
    waymark_vm_interpreter_fullset::FullSetInterpreter<TestSpec, TestExecutable, TestValue>;

pub type TestRuntime = waymark_vm_runtime::Runtime<TestExecutable, TestInterpreter, TestValue>;

pub fn compile_program(program: &waymark_vm_ast_old::Program) -> TestExecutable {
    waymark_vm_compiler_for_ast_old::compile::<TestSpec, TestLowering>(program)
        .expect("program should compile")
}

pub fn runtime(executable: TestExecutable) -> TestRuntime {
    waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        TestInterpreter::default(),
        executable,
    )
    .expect("compiled main function should exist")
}

pub fn runtime_with_args(executable: TestExecutable, args: Vec<TestReadyValue>) -> TestRuntime {
    waymark_vm_runtime::Runtime::with_custom_entrypoint(
        TestInterpreter::default(),
        executable,
        waymark_vm_runtime::CallSpec {
            func: waymark_vm_bytecode_core::FunctionId::default(),
            args: args.into_iter().map(TestValue::Ready).collect(),
        },
    )
    .expect("compiled main function should exist")
}
