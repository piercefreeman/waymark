//! Test harness for the extcallset interpreter integration tests.
//!
//! Defines a minimal [`TestSpec`], a hand-rolled promise-aware [`TestValue`],
//! and a wrapper [`RuntimeInterpreter`] that adds an `InspectPending`
//! synthetic instruction for asserting on the suspended promise state. Also
//! exposes a single-function runtime builder.
//!
//! # Why not `PromiseValue`?
//!
//! `waymark-vm-runtime-promise-value` sits *above* the interpreter crates in
//! the build graph (it depends on them), so a per-interpreter test crate
//! must not pull it back in as a dev-dependency — that would be a backward
//! edge against the established layering. Hand-roll the promise-aware value
//! type here instead. The sibling interpreter test crates (coreset,
//! pureset, fullset) follow the same rule.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_extcallset::{
    Effect, Error as InterpreterError, ExtCallSetInterpreter, RuntimeView,
};
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::{
    CaptureRuntimeView, Frame, FullRuntimeView, PromiseState, RegisterId,
};
use waymark_vm_runtime_promise_core::{
    PromiseStateId, Resolvable, Suspendable, UnresolvedPromiseError,
};
use waymark_vm_runtime_test::{FunctionId, StateId};

pub type TestRuntime = Runtime<Executable<RuntimeInstruction>, RuntimeInterpreter, TestValue>;

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ActionRef = usize;
}

#[derive(Debug, Clone)]
pub struct TestReadyValue(pub i32);

impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
    type RootValue = TestValue;
}

impl waymark_vm_runtime_exception::AsException for TestReadyValue {
    fn as_exception(
        &self,
    ) -> Result<
        &waymark_vm_runtime_exception::Exception<TestValue>,
        waymark_vm_runtime_exception::NotAnExceptionError,
    > {
        Err(waymark_vm_runtime_exception::NotAnExceptionError)
    }
}

/// Minimal promise-aware value: ready holds a [`TestReadyValue`], pending
/// holds a promise state id.
///
/// Hand-rolled on purpose: see the module-level note on why we don't reuse
/// `PromiseValue` here.
#[derive(Debug, Clone)]
pub enum TestValue {
    Ready(TestReadyValue),
    Pending(PromiseStateId),
}

impl waymark_vm_runtime_value::RootValueAccess for TestValue {
    type RootValue = Self;
}

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument for TestValue {
    type ActionCallArgument = i32;
    type Error = UnresolvedPromiseError;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        match self {
            Self::Ready(TestReadyValue(value)) => Ok(*value),
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TestSleepDurationError {
    #[error("sleep duration cannot be negative")]
    Negative,

    #[error("sleep duration must be non-zero")]
    Zero,

    #[error("sleep duration is unresolved (promise {0:?})")]
    Unresolved(PromiseStateId),
}

impl waymark_vm_interpreter_extcallset::value::SleepDuration for TestValue {
    type Error = TestSleepDurationError;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        match self {
            Self::Ready(TestReadyValue(value)) => {
                let seconds: u64 = (*value).try_into().map_err(|_| Self::Error::Negative)?;
                NonZeroDuration::from_secs(seconds).ok_or(Self::Error::Zero)
            }
            Self::Pending(promise_state_id) => Err(Self::Error::Unresolved(*promise_state_id)),
        }
    }
}

impl Suspendable for TestValue {
    fn from_pending(promise_state_id: PromiseStateId) -> Self {
        Self::Pending(promise_state_id)
    }

    fn as_pending(&self) -> Option<PromiseStateId> {
        match self {
            Self::Pending(promise_state_id) => Some(*promise_state_id),
            Self::Ready(_) => None,
        }
    }
}

impl Resolvable for TestValue {
    type ReadyValue = TestReadyValue;

    fn from_ready(value: TestReadyValue) -> Self {
        Self::Ready(value)
    }

    fn into_ready(self) -> Result<TestReadyValue, (UnresolvedPromiseError, Self)> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err((
                UnresolvedPromiseError { promise_state_id },
                Self::Pending(promise_state_id),
            )),
        }
    }

    fn as_ready(&self) -> Result<&TestReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
        }
    }

    fn as_ready_mut(&mut self) -> Result<&mut TestReadyValue, UnresolvedPromiseError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
        }
    }
}

#[derive(Debug)]
pub enum RuntimeInstruction {
    ExtCall(ExtCallSet<TestSpec>),
    InspectPending(RegisterId),
}

impl From<ExtCallSet<TestSpec>> for RuntimeInstruction {
    fn from(value: ExtCallSet<TestSpec>) -> Self {
        Self::ExtCall(value)
    }
}

#[derive(Debug)]
pub enum TestEffect {
    ExtCallSet(Effect<usize, i32>),
    PendingPromiseStateId(PromiseStateId),
    Complete,
}

impl From<Result<TestReadyValue, TestReadyValue>> for TestEffect {
    fn from(_: Result<TestReadyValue, TestReadyValue>) -> Self {
        Self::Complete
    }
}

#[derive(Default)]
pub struct RuntimeInterpreter {
    extcall: ExtCallSetInterpreter<TestSpec, FunctionId, StateId, TestValue>,
}

impl<E> CaptureRuntimeView<E, FunctionId, StateId, TestValue> for RuntimeInterpreter {
    type RuntimeView<'r>
        = RuntimeView<'r, FunctionId, StateId, TestValue>
    where
        E: 'r,
        FunctionId: 'r,
        StateId: 'r,
        TestValue: 'r;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, E, FunctionId, StateId, TestValue>,
    ) -> Self::RuntimeView<'r> {
        let FullRuntimeView { state, .. } = view;
        RuntimeView { state }
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> = RuntimeView<'r, FunctionId, StateId, TestValue>;
    type Frame = Frame<FunctionId, StateId, TestValue>;
    type Instruction = RuntimeInstruction;
    type Error = InterpreterError<TestValue>;
    type Effect = TestEffect;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match instruction {
            RuntimeInstruction::ExtCall(instruction) => {
                waymark_vm_interpreter::Interpreter::execute(
                    &self.extcall,
                    runtime_view,
                    frame,
                    instruction,
                )
                .map(|outcome| outcome.map_effect(TestEffect::ExtCallSet))
            }
            RuntimeInstruction::InspectPending(register) => {
                let RuntimeView { state } = runtime_view;

                let TestValue::Pending(promise_state_id) = frame.regs[*register].clone() else {
                    panic!("register should hold the suspended operation's pending promise");
                };

                assert!(matches!(
                    state.promise_states.get(promise_state_id),
                    Ok(PromiseState::Waiting(waiters)) if waiters.is_empty()
                ));

                Ok(ExecutionOutcome::ExitFrameWithEffect(
                    TestEffect::PendingPromiseStateId(promise_state_id),
                ))
            }
        }
    }
}

/// Build a runtime that enters [`FunctionId(0)`] with the given ready
/// arguments.
pub fn new_runtime_with_args(
    executable: Executable<RuntimeInstruction>,
    args: Vec<TestReadyValue>,
) -> TestRuntime {
    Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: args.into_iter().map(TestValue::Ready).collect(),
        },
    )
    .expect("function 0 should exist")
}
