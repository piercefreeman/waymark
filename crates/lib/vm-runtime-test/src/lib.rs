//! Universal test fixtures for the VM runtime.
//!
//! Everything in this crate must remain agnostic to any specific instruction
//! set or interpreter (coreset, pureset, extcallset, fullset, …). The intent
//! is that adding a new instruction, effect, or value-trait to one of those
//! interpreters never forces a change here.
//!
//! Concretely, this crate may depend on the core runtime traits
//! ([`waymark_vm_interpreter::Interpreter`],
//! [`waymark_vm_runtime_core::CaptureRuntimeView`],
//! [`waymark_vm_interpreter_coreset::value::CaptureCallArgument`], etc.) that
//! every interpreter is expected to honor, but it must not depend on the
//! per-instruction-set crates (e.g. `waymark-vm-instructions-pureset`,
//! `waymark-vm-interpreter-extcallset`).
//!
//! What belongs here:
//!
//! - Generic builder helpers ([`function`], [`executable`]) that don't know
//!   about any particular instruction set.
//! - Re-exports of universal id types ([`FunctionId`], [`StateId`]).
//! - A self-contained [`TestInstruction`]/[`TestInterpreter`]/[`TestRuntime`]
//!   fixture used by `vm-runtime`'s own tests — its surface only changes when
//!   the core runtime traits change.
//!
//! What does NOT belong here:
//!
//! - Trait impls for any `<instruction-set>::value::*` trait that isn't a
//!   universal contract.
//! - `Spec` types or value enums tailored to a specific interpreter's needs.
//! - Per-interpreter `TestSpec`/`TestValue` aliases — those live in each
//!   interpreter crate's `tests/support/mod.rs`.

use serde::{Deserialize, Serialize};
use waymark_vm_interpreter::{ExecutionOutcome, Interpreter};
use waymark_vm_runtime::{CallSpec, FunctionNotFoundError, Runtime};
use waymark_vm_runtime_core::{
    CaptureRuntimeView, Continuation, ExceptionHandlers, Frame, FrameKind, FullRuntimeView,
    PromiseState, RegisterId, Registers,
};
use waymark_vm_runtime_exception::{Exception, FromException};

pub use waymark_vm_bytecode_core::{FunctionId, StateId};
use waymark_vm_runtime_promise_value::PromiseValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestInstruction {
    Emit(&'static str),
    Exit,
    Suspend {
        dst: RegisterId,
        resume: StateId,
    },
    EmitRegister(RegisterId),
    EmitException,
    EnqueueFrame {
        func: FunctionId,
        state: StateId,
        num_regs: usize,
    },
    EnqueueFrameAndExit {
        func: FunctionId,
        state: StateId,
        num_regs: usize,
    },
    Fail(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestEffect {
    Message(&'static str),
    Value(TestReadyValue),
    UnhandledException(Exception<TestReadyValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TestExecutionError(pub &'static str);

pub struct TestInterpreter;

pub type TestExecutable = waymark_vm_bytecode::Executable<TestInstruction>;

pub type TestFunction = waymark_vm_bytecode::Function<TestInstruction>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestReadyValue {
    Int(i32),
    Exception(Box<Exception<PromiseValue<TestReadyValue>>>),
}
pub type TestValue = PromiseValue<TestReadyValue>;

impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
    type RootValue = TestValue;
}

impl FromException for TestReadyValue {
    fn from_exception(exception: Exception<Self::RootValue>) -> Self {
        Self::Exception(Box::new(exception))
    }
}

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestReadyValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

pub fn function<Instruction>(
    num_regs: usize,
    states: Vec<Vec<Instruction>>,
) -> waymark_vm_bytecode::Function<Instruction> {
    waymark_vm_bytecode::Function {
        states: states
            .into_iter()
            .map(|instructions| waymark_vm_bytecode::State {
                instructions: instructions.into_iter().collect(),
            })
            .collect(),
        num_regs,
    }
}

pub fn executable<Instruction>(
    functions: Vec<waymark_vm_bytecode::Function<Instruction>>,
) -> waymark_vm_bytecode::Executable<Instruction> {
    waymark_vm_bytecode::Executable {
        functions: functions.into_iter().collect(),
    }
}

impl CaptureRuntimeView<TestExecutable, FunctionId, StateId, TestValue> for TestInterpreter {
    type RuntimeView<'r> = FullRuntimeView<'r, TestExecutable, FunctionId, StateId, TestValue>;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, TestExecutable, FunctionId, StateId, TestValue>,
    ) -> Self::RuntimeView<'r> {
        view
    }
}

impl Interpreter for TestInterpreter {
    type RuntimeView<'r> = FullRuntimeView<'r, TestExecutable, FunctionId, StateId, TestValue>;
    type Frame = Frame<FunctionId, StateId, TestValue>;
    type Instruction = TestInstruction;
    type Error = TestExecutionError;
    type Effect = TestEffect;

    fn execute<'r>(
        &self,
        runtime: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match *instruction {
            TestInstruction::Emit(message) => Ok(ExecutionOutcome::ExitFrameWithEffect(
                TestEffect::Message(message),
            )),
            TestInstruction::Exit => Ok(ExecutionOutcome::ExitFrame),
            TestInstruction::Suspend { dst, resume } => {
                let FullRuntimeView { state, .. } = runtime;
                let promise_state_id = state.promise_states.prepare();
                let promise_state = state
                    .promise_states
                    .get_mut(promise_state_id)
                    .expect("prepared promise state should exist");
                *promise_state =
                    PromiseState::Waiting(vec![waymark_vm_runtime_core::PromiseWaiter::Await(
                        Continuation::capture(frame, resume, dst),
                    )]);
                Ok(ExecutionOutcome::ExitFrame)
            }
            TestInstruction::EmitRegister(register) => {
                let Some(PromiseValue::Ready(value)) = frame.regs.get(register) else {
                    panic!("register should hold a resolved value before emitting it");
                };
                Ok(ExecutionOutcome::ExitFrameWithEffect(TestEffect::Value(
                    value.clone(),
                )))
            }
            TestInstruction::EmitException => {
                let Some(exception) = frame.exception else {
                    panic!("frame should carry a raised exception before emitting it");
                };
                let Exception { type_id, details } = exception;
                let PromiseValue::Ready(details) = details else {
                    panic!("raised exception details should be ready values");
                };
                Ok(ExecutionOutcome::ExitFrameWithEffect(
                    TestEffect::UnhandledException(Exception { type_id, details }),
                ))
            }
            TestInstruction::EnqueueFrame {
                func,
                state: next_state,
                num_regs,
            } => {
                let FullRuntimeView { state, .. } = runtime;
                state.ready.push_back(Frame {
                    func,
                    state: next_state,
                    regs: Registers::new(num_regs),
                    exception: None,
                    exception_handler_blocks: ExceptionHandlers::new(),
                    kind: FrameKind::TopLevel,
                });
                Ok(ExecutionOutcome::Continue(frame))
            }
            TestInstruction::EnqueueFrameAndExit {
                func,
                state: next_state,
                num_regs,
            } => {
                let FullRuntimeView { state, .. } = runtime;
                state.ready.push_back(Frame {
                    func,
                    state: next_state,
                    regs: Registers::new(num_regs),
                    exception: None,
                    exception_handler_blocks: ExceptionHandlers::new(),
                    kind: FrameKind::TopLevel,
                });
                Ok(ExecutionOutcome::ExitFrame)
            }
            TestInstruction::Fail(message) => Err(TestExecutionError(message)),
        }
    }
}

pub type TestRuntime = Runtime<TestExecutable, TestInterpreter, TestValue>;

pub fn try_runtime(
    executable: TestExecutable,
) -> Result<TestRuntime, FunctionNotFoundError<FunctionId>> {
    Runtime::with_conventional_entrypoint(TestInterpreter, executable)
}

pub fn runtime(executable: TestExecutable) -> TestRuntime {
    try_runtime(executable).expect("entrypoint should exist")
}

pub fn try_runtime_with_entrypoint<Arg>(
    executable: TestExecutable,
    call: CallSpec<FunctionId, Arg>,
) -> Result<TestRuntime, FunctionNotFoundError<FunctionId>>
where
    Arg: Into<TestValue>,
{
    Runtime::with_custom_entrypoint(TestInterpreter, executable, call)
}

pub fn runtime_with_entrypoint<Arg>(
    executable: TestExecutable,
    call: CallSpec<FunctionId, Arg>,
) -> TestRuntime
where
    Arg: Into<TestValue>,
{
    try_runtime_with_entrypoint(executable, call)
        .expect("test executable should define the entrypoint")
}

pub fn value_ready(value: TestReadyValue) -> TestValue {
    PromiseValue::Ready(value)
}
