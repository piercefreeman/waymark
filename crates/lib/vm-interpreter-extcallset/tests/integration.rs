use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_extcallset::{
    Effect, Error as InterpreterError, ExtCallSetInterpreter, RuntimeView, SleepError,
};
use waymark_vm_runtime::{CallSpec, RunError, Runtime};
use waymark_vm_runtime_core::{
    CaptureRuntimeView, Frame, FullRuntimeView, PromiseState, RegisterId,
};
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_runtime_promise_value::PromiseValue;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ActionRef = usize;
}

#[derive(Debug, Clone)]
struct TestReadyValue(i32);
type TestValue = PromiseValue<TestReadyValue>;

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for TestReadyValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}

impl waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument for TestReadyValue {
    type ActionCallArgument = i32;
    type Error = waymark_vm_runtime_promise_core::UnresolvedPromiseError;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        let Self(value) = self;
        Ok(*value)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum TestSleepDurationError {
    #[error("sleep duration cannot be negative")]
    Negative,

    #[error("sleep duration must be non-zero")]
    Zero,
}

impl waymark_vm_interpreter_extcallset::value::SleepDuration for TestReadyValue {
    type Error = TestSleepDurationError;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        let seconds: u64 = self.0.try_into().map_err(|_| Self::Error::Negative)?;
        NonZeroDuration::from_secs(seconds).ok_or(Self::Error::Zero)
    }
}

#[derive(Debug)]
enum RuntimeInstruction {
    ExtCall(ExtCallSet<TestSpec>),
    InspectPending(RegisterId),
}

impl From<ExtCallSet<TestSpec>> for RuntimeInstruction {
    fn from(value: ExtCallSet<TestSpec>) -> Self {
        Self::ExtCall(value)
    }
}

#[derive(Debug)]
enum TestEffect {
    ExtCallSet(Effect<usize, i32>),
    PendingPromiseStateId(PromiseStateId),
}

#[derive(Default)]
struct RuntimeInterpreter {
    extcall: ExtCallSetInterpreter<TestSpec, FunctionId, StateId, TestValue>,
}

impl<Executable> CaptureRuntimeView<Executable, FunctionId, StateId, TestValue>
    for RuntimeInterpreter
{
    type RuntimeView<'r>
        = RuntimeView<'r, FunctionId, StateId, TestValue>
    where
        Executable: 'r,
        FunctionId: 'r,
        StateId: 'r,
        TestValue: 'r;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, Executable, FunctionId, StateId, TestValue>,
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

                let PromiseValue::Pending(promise_state_id) = frame.regs[*register].clone() else {
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

#[test]
fn runtime_emits_an_action_call_and_queues_the_resumed_frame() {
    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![
                vec![
                    ExtCallSet::ActionCall {
                        dst: RegisterId(1),
                        action_ref: 7,
                        args: vec![RegisterId(0)],
                        resume: StateId(1),
                    }
                    .into(),
                ],
                vec![RuntimeInstruction::InspectPending(RegisterId(1))],
            ],
        )]),
        CallSpec {
            func: FunctionId(0),
            args: vec![TestValue::Ready(TestReadyValue(41))],
        },
    )
    .expect("function 0 should exist");

    let TestEffect::ExtCallSet(Effect::ActionCall {
        promise_state_id,
        action_ref,
        args,
    }) = runtime
        .run()
        .expect("first run should emit the action call")
    else {
        panic!("first run should emit an action call");
    };

    assert_eq!(action_ref, 7);
    assert_eq!(args, vec![41]);

    let TestEffect::PendingPromiseStateId(resumed_promise_state_id) = runtime
        .run()
        .expect("second run should execute the resumed frame")
    else {
        panic!("second run should inspect the resumed pending promise");
    };

    assert_eq!(resumed_promise_state_id, promise_state_id);
}

#[test]
fn runtime_emits_a_sleep_effect_and_queues_the_resumed_frame() {
    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![
                vec![
                    ExtCallSet::Sleep {
                        dst: RegisterId(1),
                        duration: RegisterId(0),
                        resume: StateId(1),
                    }
                    .into(),
                ],
                vec![RuntimeInstruction::InspectPending(RegisterId(1))],
            ],
        )]),
        CallSpec {
            func: FunctionId(0),
            args: vec![TestValue::Ready(TestReadyValue(5))],
        },
    )
    .expect("function 0 should exist");

    let TestEffect::ExtCallSet(Effect::Sleep {
        promise_state_id,
        duration,
    }) = runtime
        .run()
        .expect("first run should emit the sleep effect")
    else {
        panic!("first run should emit a sleep effect");
    };

    assert_eq!(duration, NonZeroDuration::from_secs(5).unwrap());

    let TestEffect::PendingPromiseStateId(resumed_promise_state_id) = runtime
        .run()
        .expect("second run should execute the resumed frame")
    else {
        panic!("second run should inspect the resumed pending promise");
    };

    assert_eq!(resumed_promise_state_id, promise_state_id);
}

#[test]
fn runtime_surfaces_invalid_sleep_duration_errors_from_the_interpreter() {
    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![vec![
                ExtCallSet::Sleep {
                    dst: RegisterId(1),
                    duration: RegisterId(0),
                    resume: StateId(1),
                }
                .into(),
            ]],
        )]),
        CallSpec {
            func: FunctionId(0),
            args: vec![TestValue::Ready(TestReadyValue(0))],
        },
    )
    .expect("function 0 should exist");

    assert!(matches!(
        runtime.run(),
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            InterpreterError::Sleep(SleepError::InvalidDuration {
                source: waymark_vm_runtime_promise_value::Error::Ready(
                    TestSleepDurationError::Zero
                ),
            })
        )))
    ));
}
