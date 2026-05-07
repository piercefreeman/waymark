use waymark_vm_compiler_for_ast_old_test_support::{
    TestConstValue, TestExecutable, TestLowering, TestSpec,
};

type TestInterpreter =
    waymark_vm_interpreter_fullset::FullSetInterpreter<TestSpec, TestExecutable, TestValue>;

pub type TestRuntime = waymark_vm_runtime::Runtime<TestExecutable, TestInterpreter, TestValue>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Int(i64),
    None,
    List(Vec<TestValue>),
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(match self {
            Self::Int(value) => *value != 0,
            Self::None => false,
            Self::List(_) => false,
        })
    }
}

impl waymark_vm_interpreter_pureset::value::Add for TestValue {
    fn add(a: &Self, b: &Self) -> Result<Self, waymark_vm_interpreter_pureset::value::AddError> {
        match (a, b) {
            (Self::Int(a), Self::Int(b)) => Ok(Self::Int(a + b)),
            _ => Err(waymark_vm_interpreter_pureset::value::AddError::NotAddable),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::MakeList for TestValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self>,
    {
        Ok(Self::List(items.into_iter().collect()))
    }
}

impl From<TestConstValue> for TestValue {
    fn from(value: TestConstValue) -> Self {
        match value {
            TestConstValue::Int(value) => Self::Int(value),
            TestConstValue::None => Self::None,
        }
    }
}

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

pub fn runtime_with_args(executable: TestExecutable, args: Vec<TestValue>) -> TestRuntime {
    waymark_vm_runtime::Runtime::with_custom_entrypoint(
        TestInterpreter::default(),
        executable,
        waymark_vm_runtime::CallSpec {
            func: waymark_vm_bytecode_core::FunctionId::default(),
            args,
        },
    )
    .expect("compiled main function should exist")
}
