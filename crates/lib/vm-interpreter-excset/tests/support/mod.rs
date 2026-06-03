//! Test harness for the excset interpreter integration tests.
//!
//! Defines a minimal spec, a value type that implements the excset value
//! traits, and a wrapper runtime interpreter that adds synthetic instructions
//! for seeding registers and emitting terminal values.

use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_excset::{
    Effect as ExcEffect, Error as InterpreterError, ExcSetInterpreter,
};
use waymark_vm_runtime::{RunError, Runtime};
use waymark_vm_runtime_core::{CaptureRuntimeView as _, Frame, FullRuntimeView, RegisterId};
use waymark_vm_runtime_promise_core::{PromiseStateId, Resolvable, UnresolvedPromiseError};
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

type RuntimeExecutable = waymark_vm_bytecode::Executable<RuntimeInstruction>;

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
    Pending(PromiseStateId),
}

impl TestValue {
    pub fn exception(type_id: &'static str, details: TestValue) -> Self {
        Self::Exception(Box::new(waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details,
            bubble: true,
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
                a.type_id == b.type_id && a.details == b.details && a.bubble == b.bubble
            }
            (Self::Pending(a), Self::Pending(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for TestValue {}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = Self;
}

impl Resolvable for TestValue {
    type ReadyValue = Self;

    fn from_ready(value: Self::ReadyValue) -> Self {
        value
    }

    fn into_ready(self) -> Result<Self::ReadyValue, (UnresolvedPromiseError, Self)> {
        match self {
            Self::Pending(promise_state_id) => Err((
                UnresolvedPromiseError { promise_state_id },
                Self::Pending(promise_state_id),
            )),
            value => Ok(value),
        }
    }

    fn as_ready(&self) -> Result<&Self::ReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
            value => Ok(value),
        }
    }

    fn as_ready_mut(&mut self) -> Result<&mut Self::ReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
            value => Ok(value),
        }
    }
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
            Self::Bool(_) | Self::Int(_) | Self::Text(_) | Self::Pending(_) => {
                Err(waymark_vm_runtime_exception::NotAnExceptionError)
            }
        }
    }
}

impl waymark_vm_runtime_exception::AsExceptionMut for TestValue {
    fn as_exception_mut(
        &mut self,
    ) -> Result<
        &mut waymark_vm_runtime_exception::Exception<Self::RootValue>,
        waymark_vm_runtime_exception::NotAnExceptionError,
    > {
        match self {
            Self::Exception(exception) => Ok(exception.as_mut()),
            Self::Bool(_) | Self::Int(_) | Self::Text(_) | Self::Pending(_) => {
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
            Self::Bool(_) | Self::Int(_) | Self::Exception(_) | Self::Pending(_) => {
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

impl waymark_vm_interpreter_excset::value::FromShouldBubble for TestValue {
    fn from_should_bubble(should_bubble: bool) -> Self::RootValue {
        Self::Bool(should_bubble)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeEffect {
    Register(TestValue),
    Complete(Result<TestValue, TestValue>),
}

impl From<Result<TestValue, TestValue>> for RuntimeEffect {
    fn from(value: Result<TestValue, TestValue>) -> Self {
        Self::Complete(value)
    }
}

impl From<ExcSet<TestSpec>> for RuntimeInstruction {
    fn from(value: ExcSet<TestSpec>) -> Self {
        Self::Exc(value)
    }
}

#[derive(Default)]
pub struct RuntimeInterpreter {
    exc: ExcSetInterpreter<TestSpec, RuntimeExecutable, FunctionId, StateId, TestValue>,
}

impl<Executable>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, FunctionId, StateId, TestValue>
    for RuntimeInterpreter
{
    type RuntimeView<'v>
        = FullRuntimeView<'v, Executable, FunctionId, StateId, TestValue>
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        TestValue: 'v;

    fn capture_runtime_view<'r>(
        view: waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            TestValue,
        >,
    ) -> Self::RuntimeView<'r> {
        view
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> = FullRuntimeView<'r, RuntimeExecutable, FunctionId, StateId, TestValue>;
    type Frame = Frame<FunctionId, StateId, TestValue>;
    type Instruction = RuntimeInstruction;
    type Error = InterpreterError;
    type Effect = RuntimeEffect;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match instruction {
            RuntimeInstruction::Exc(instruction) => waymark_vm_interpreter::Interpreter::execute(
                &self.exc,
                waymark_vm_interpreter_excset::ExcSetInterpreter::<
                    TestSpec,
                    RuntimeExecutable,
                    FunctionId,
                    StateId,
                    TestValue,
                >::capture_runtime_view(runtime_view),
                frame,
                instruction,
            )
            .map(|outcome| {
                outcome.map_effect(|effect| match effect {
                    ExcEffect::Complete(result) => RuntimeEffect::Complete(result),
                })
            }),
            RuntimeInstruction::SetValue { dst, value } => {
                frame.regs.set(*dst, value.clone());
                Ok(ExecutionOutcome::Continue(frame))
            }
            RuntimeInstruction::EmitRegister(register) => {
                let value = frame.regs[*register].clone();
                Ok(ExecutionOutcome::ExitFrameWithEffect(
                    RuntimeEffect::Register(value),
                ))
            }
        }
    }
}

pub fn run(
    regs: usize,
    instrs: Vec<RuntimeInstruction>,
) -> Result<RuntimeEffect, RunError<InterpreterError>> {
    let exec = executable(vec![function::<RuntimeInstruction>(regs, vec![instrs])]);
    run_executable(exec)
}

pub fn run_executable(
    exec: RuntimeExecutable,
) -> Result<RuntimeEffect, RunError<InterpreterError>> {
    let mut runtime = Runtime::with_conventional_entrypoint(RuntimeInterpreter::default(), exec)
        .expect("function 0 should exist");
    runtime.run()
}
