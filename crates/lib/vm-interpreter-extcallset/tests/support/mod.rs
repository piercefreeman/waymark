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
    Frame, Promisable, PromisableReadiness, PromisableReadinessMut, PromisableReadinessRef,
    PromiseState, PromiseStateHandle, RegisterId,
};
use waymark_vm_runtime_promise_core::{PromiseStateId, UnresolvedPromiseError};
use waymark_vm_runtime_test::{FunctionId, StateId};

/// The extcallset raises nothing of its own, so the raised exception is
/// [`core::convert::Infallible`].
pub type TestRuntime = Runtime<
    Executable<RuntimeInstruction>,
    RuntimeInterpreter,
    TestValue,
    core::convert::Infallible,
>;

#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ActionRef = usize;
}

#[derive(Debug, Clone)]
pub struct TestMaterialValue(pub i32);

/// Minimal promise-aware value: ready holds a [`TestMaterialValue`], pending
/// holds a promise state handle.
///
/// Hand-rolled on purpose: see the module-level note on why we don't reuse
/// `PromiseValue` here.
#[derive(Debug, Clone)]
pub enum TestValue {
    Ready(TestMaterialValue),
    Pending(PromiseStateHandle),
}

impl waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument for TestValue {
    type ActionCallArgument = i32;
    type Error = UnresolvedPromiseError<PromiseStateId>;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        match self {
            Self::Ready(TestMaterialValue(value)) => Ok(*value),
            Self::Pending(promise_state_handle) => Err(UnresolvedPromiseError {
                pending: promise_state_handle.id(),
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
            Self::Ready(TestMaterialValue(value)) => {
                let seconds: u64 = (*value).try_into().map_err(|_| Self::Error::Negative)?;
                NonZeroDuration::from_secs(seconds).ok_or(Self::Error::Zero)
            }
            Self::Pending(promise_state_handle) => {
                Err(Self::Error::Unresolved(promise_state_handle.id()))
            }
        }
    }
}

impl Promisable for TestValue {
    type ReadyValue = TestMaterialValue;

    fn from_ready(value: TestMaterialValue) -> Self {
        Self::Ready(value)
    }

    fn from_pending(promise_state_handle: PromiseStateHandle) -> Self {
        Self::Pending(promise_state_handle)
    }

    fn promisable_readiness(&self) -> PromisableReadinessRef<'_, TestMaterialValue> {
        match self {
            Self::Ready(value) => PromisableReadinessRef::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadinessRef::Pending(promise_state_handle)
            }
        }
    }

    fn promisable_readiness_mut(&mut self) -> PromisableReadinessMut<'_, TestMaterialValue> {
        match self {
            Self::Ready(value) => PromisableReadinessMut::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadinessMut::Pending(promise_state_handle)
            }
        }
    }

    fn into_promisable_readiness(self) -> PromisableReadiness<TestMaterialValue> {
        match self {
            Self::Ready(value) => PromisableReadiness::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadiness::Pending(promise_state_handle)
            }
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
}

#[derive(Default)]
pub struct RuntimeInterpreter {
    extcall:
        ExtCallSetInterpreter<TestSpec, FunctionId, StateId, TestValue, core::convert::Infallible>,
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> =
        RuntimeView<'r, FunctionId, StateId, TestValue, core::convert::Infallible>;
    type Frame = Frame<FunctionId, StateId, TestValue, core::convert::Infallible>;
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

                let TestValue::Pending(promise_state_handle) = frame.regs[*register].clone() else {
                    panic!("register should hold the suspended operation's pending promise");
                };
                let promise_state_id = promise_state_handle.id();

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
    args: Vec<TestMaterialValue>,
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
