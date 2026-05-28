//! Test harness for the excset interpreter integration tests.
//!
//! Defines a minimal spec, a value type that implements the excset value
//! traits, and a wrapper runtime interpreter that adds synthetic instructions
//! for seeding registers and emitting terminal values.

use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_excset::{Error as InterpreterError, ExcSetInterpreter};
use waymark_vm_runtime::{RunError, Runtime};
use waymark_vm_runtime_core::{Frame, RegisterId};
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_excset::Spec for TestSpec {
    type RegisterId = RegisterId;
}

#[derive(Debug, Clone)]
pub enum TestValue {
    Bool(bool),
    Int(i64),
    Text(&'static str),
    Exception(Box<waymark_vm_runtime_exception::Exception<TestValue>>),
}

impl TestValue {
    pub fn exception(type_id: &'static str, details: TestValue) -> Self {
        Self::Exception(Box::new(waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details,
        }))
    }
}

impl PartialEq for TestValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Exception(a), Self::Exception(b)) => {
                a.type_id == b.type_id && a.details == b.details
            }
            _ => false,
        }
    }
}

impl Eq for TestValue {}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = Self;
}

static_assertions::assert_impl_all!(TestValue: waymark_vm_interpreter_excset::Value);

impl waymark_vm_runtime_exception::AsException for TestValue {
    fn as_exception(
        &self,
    ) -> Result<
        &waymark_vm_runtime_exception::Exception<Self::RootValue>,
        waymark_vm_runtime_exception::NotAnExceptionError,
    > {
        match self {
            Self::Exception(exception) => Ok(exception.as_ref()),
            Self::Bool(_) | Self::Int(_) | Self::Text(_) => {
                Err(waymark_vm_runtime_exception::NotAnExceptionError)
            }
        }
    }
}

impl waymark_vm_interpreter_excset::value::AsExceptionTypeId for TestValue {
    fn as_exception_type_id(
        &self,
    ) -> Result<&str, waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError> {
        match self {
            Self::Text(value) => Ok(value),
            Self::Bool(_) | Self::Int(_) | Self::Exception(_) => {
                Err(waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError)
            }
        }
    }
}

impl waymark_vm_interpreter_excset::value::FromIsException for TestValue {
    fn from_is_exception(is_exception: bool) -> Self::RootValue {
        Self::Bool(is_exception)
    }
}

impl waymark_vm_interpreter_excset::value::CaptureExceptionDetails for TestValue {
    fn from_exception_details(value: &Self::RootValue) -> Self::RootValue {
        value.clone()
    }
}

#[derive(Debug)]
pub enum RuntimeInstruction {
    Exc(ExcSet<TestSpec>),
    SetValue { dst: RegisterId, value: TestValue },
    EmitRegister(RegisterId),
}

impl From<ExcSet<TestSpec>> for RuntimeInstruction {
    fn from(value: ExcSet<TestSpec>) -> Self {
        Self::Exc(value)
    }
}

#[derive(Default)]
pub struct RuntimeInterpreter {
    exc: ExcSetInterpreter<TestSpec, FunctionId, StateId, TestValue>,
}

impl<Executable>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, FunctionId, StateId, TestValue>
    for RuntimeInterpreter
{
    type RuntimeView<'v>
        = ()
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        TestValue: 'v;

    fn capture_runtime_view<'r>(
        _view: waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            TestValue,
        >,
    ) -> Self::RuntimeView<'r> {
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> = ();
    type Frame = Frame<FunctionId, StateId, TestValue>;
    type Instruction = RuntimeInstruction;
    type Error = InterpreterError;
    type Effect = TestValue;

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match instruction {
            RuntimeInstruction::Exc(instruction) => {
                waymark_vm_interpreter::Interpreter::execute(&self.exc, (), frame, instruction)
                    .map(|outcome| outcome.map_effect(|effect| match effect {}))
            }
            RuntimeInstruction::SetValue { dst, value } => {
                frame.regs.set(*dst, value.clone());
                Ok(ExecutionOutcome::Continue(frame))
            }
            RuntimeInstruction::EmitRegister(register) => {
                let value = frame.regs[*register].clone();
                Ok(ExecutionOutcome::ExitFrameWithEffect(value))
            }
        }
    }
}

pub fn run(
    regs: usize,
    instrs: Vec<RuntimeInstruction>,
) -> Result<TestValue, RunError<InterpreterError>> {
    let exec = executable(vec![function::<RuntimeInstruction>(regs, vec![instrs])]);
    let mut runtime = Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), exec)
        .expect("function 0 should exist");
    runtime.run()
}
