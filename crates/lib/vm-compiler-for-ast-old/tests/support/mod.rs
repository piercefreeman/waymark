use waymark_vm_compiler_for_ast_old::compile;

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode_core::FunctionId;
    type StateId = waymark_vm_bytecode_core::StateId;
    type ExtCallId = TestExtCallId;
}

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type ConstValue = TestConstValue;
}

pub type TestExecutable = waymark_vm_compiler_for_ast_old::ExecutableFor<TestSpec>;

type TestInterpreter =
    waymark_vm_interpreter_fullset::FullSetInterpreter<TestSpec, TestExecutable, TestValue>;

pub type TestRuntime = waymark_vm_runtime::Runtime<TestExecutable, TestInterpreter, TestValue>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestConstValue {
    Int(i64),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestExtCallId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestValue {
    Int(i64),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestLiteralLoweringError {
    UnsupportedLiteral,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestActionLoweringError {}

pub struct TestLowering;

impl<Spec> waymark_vm_compiler_for_ast_old::lowering::CoreSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_coreset::Spec<ExtCallId = TestExtCallId>,
{
    type ActionError = TestActionLoweringError;

    fn lower_action(
        call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<Spec::ExtCallId, Self::ActionError> {
        Ok(TestExtCallId(call.action_name.clone()))
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old::lowering::PureSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = TestConstValue>,
{
    type LiteralError = TestLiteralLoweringError;

    fn lower_literal(
        literal: &waymark_vm_ast_old::Literal,
    ) -> Result<Spec::ConstValue, Self::LiteralError> {
        use waymark_vm_ast_old::Literal;
        match literal {
            Literal::Int(value) => Ok(TestConstValue::Int(*value)),
            Literal::None => Ok(TestConstValue::None),
            Literal::Float(_) | Literal::String(_) | Literal::Bool(_) => {
                Err(TestLiteralLoweringError::UnsupportedLiteral)
            }
        }
    }
}

impl waymark_vm_interpreter_coreset::value::ShouldJump for TestValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(match self {
            Self::Int(value) => *value != 0,
            Self::None => false,
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

impl From<TestConstValue> for TestValue {
    fn from(value: TestConstValue) -> Self {
        match value {
            TestConstValue::Int(value) => Self::Int(value),
            TestConstValue::None => Self::None,
        }
    }
}

pub fn compile_program(program: &waymark_vm_ast_old::Program) -> TestExecutable {
    compile::<TestSpec, TestLowering>(program).expect("program should compile")
}

pub fn runtime(executable: TestExecutable) -> TestRuntime {
    waymark_vm_runtime::Runtime::with_conventional_entrypoint(
        TestInterpreter::default(),
        executable,
    )
    .expect("compiled main function should exist")
}
