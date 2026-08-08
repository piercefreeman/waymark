//! Integration test for completing extcall action calls with exception errors.
//!
//! Runs the extcallset interpreter over the concrete `vm-value` types with
//! the provided operations implementations from this crate (any variation
//! marker selects them; a local one is used).

use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter::CaptureRuntimeView;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_coreset::CoreSetInterpreter;
use waymark_vm_interpreter_extcallset::ExtCallSetInterpreter;
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::{Frame, FullRuntimeView, RegisterId};
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};
use waymark_vm_value::{ReadyValue, Value};

/// A local variation marker: the provided operations implementations are
/// blankets over every variation, so any marker selects them.
enum TestVariation {}

type TestOperations = waymark_vm_interpreter_operations::Operations<TestVariation>;

/// The coreset truthiness semantics are variation-supplied; this test
/// exercises the extcall path, so any total definition will do.
impl waymark_vm_interpreter_coreset::operations::ShouldJump<ReadyValue> for TestVariation {
    type Error = core::convert::Infallible;

    fn should_jump(value: &ReadyValue) -> Result<bool, Self::Error> {
        Ok(!matches!(value, ReadyValue::None))
    }
}

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type FunctionId = FunctionId;
    type StateId = StateId;
}

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ActionRef = usize;
}

#[derive(Debug)]
enum Instruction {
    Core(CoreSet<TestSpec>),
    ExtCallSet(ExtCallSet<TestSpec>),
}

impl From<CoreSet<TestSpec>> for Instruction {
    fn from(value: CoreSet<TestSpec>) -> Self {
        Self::Core(value)
    }
}

impl From<ExtCallSet<TestSpec>> for Instruction {
    fn from(value: ExtCallSet<TestSpec>) -> Self {
        Self::ExtCallSet(value)
    }
}

#[derive(Debug)]
enum TestEffect {
    CoreSet(waymark_vm_interpreter_coreset::Effect<ReadyValue>),
    ExtCallSet(waymark_vm_interpreter_extcallset::Effect<usize, ReadyValue>),
}

#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error(transparent)]
    CoreSet(#[from] waymark_vm_interpreter_coreset::Error<TestSpec, TestOperations, Value>),

    #[error(transparent)]
    ExtCallSet(#[from] waymark_vm_interpreter_extcallset::Error<TestOperations, Value>),
}

#[derive(Default)]
struct RuntimeInterpreter {
    core_set: CoreSetInterpreter<TestSpec, Executable<Instruction>, TestOperations, Value>,
    extcall_set: ExtCallSetInterpreter<TestSpec, FunctionId, StateId, TestOperations, Value>,
}

impl<'r>
    CaptureRuntimeView<'r, FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>>
    for RuntimeInterpreter
{
    type Captured =
        &'r mut FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>;

    fn capture_runtime_view(
        source: &'r mut FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>,
    ) -> Self::Captured {
        source
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> =
        &'r mut FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>;
    type Frame = Frame<FunctionId, StateId, Value>;
    type Instruction = Instruction;
    type Error = TestError;
    type Effect = TestEffect;

    fn enter_state<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        let state_before = frame.state;
        let sub_runtime_view = CoreSetInterpreter::<
            TestSpec,
            Executable<Instruction>,
            TestOperations,
            Value,
        >::capture_runtime_view(&mut *runtime_view);
        let outcome = waymark_vm_interpreter::Interpreter::enter_state(
            &self.core_set,
            sub_runtime_view,
            frame,
        )
        .map_err(TestError::from)?
        .map_effect(TestEffect::CoreSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        let sub_runtime_view = ExtCallSetInterpreter::<
            TestSpec,
            FunctionId,
            StateId,
            TestOperations,
            Value,
        >::capture_runtime_view(&mut *runtime_view);
        let outcome = waymark_vm_interpreter::Interpreter::enter_state(
            &self.extcall_set,
            sub_runtime_view,
            frame,
        )
        .map_err(TestError::from)?
        .map_effect(TestEffect::ExtCallSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                Ok(ExecutionOutcome::Continue(next_frame))
            }
            outcome => Ok(outcome),
        }
    }

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match instruction {
            Instruction::Core(instruction) => {
                let runtime_view = CoreSetInterpreter::<
                    TestSpec,
                    Executable<Instruction>,
                    TestOperations,
                    Value,
                >::capture_runtime_view(runtime_view);
                self.core_set
                    .execute(runtime_view, frame, instruction)
                    .map(|outcome| outcome.map_effect(TestEffect::CoreSet))
                    .map_err(TestError::from)
            }
            Instruction::ExtCallSet(instruction) => {
                let runtime_view = ExtCallSetInterpreter::<
                    TestSpec,
                    FunctionId,
                    StateId,
                    TestOperations,
                    Value,
                >::capture_runtime_view(runtime_view);
                self.extcall_set
                    .execute(runtime_view, frame, instruction)
                    .map(|outcome| outcome.map_effect(TestEffect::ExtCallSet))
                    .map_err(TestError::from)
            }
        }
    }
}

#[test]
fn action_call_can_resume_with_an_exception_error() {
    let executable = executable(vec![function::<Instruction>(
        3,
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
            vec![
                CoreSet::Await {
                    dst: RegisterId(2),
                    src: RegisterId(1),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(2) }.into()],
        ],
    )]);

    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable,
        CallSpec {
            func: FunctionId(0),
            args: vec![ReadyValue::Int(41)],
        },
    )
    .expect("function 0 should exist");

    let emitted_effect = runtime
        .run()
        .expect("first run should emit the action call");
    let TestEffect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
        promise_state_id,
        action_ref,
        args,
    }) = emitted_effect.effect
    else {
        panic!("first run should emit an action call");
    };

    assert_eq!(action_ref, 7);
    assert_eq!(args, vec![ReadyValue::Int(41)]);

    runtime
        .reject_promise(
            promise_state_id,
            Exception {
                type_id: "ValueError".to_owned(),
                details: ReadyValue::String("boom".to_owned()),
            },
        )
        .expect("action call promise should reject cleanly");

    let emitted_effect = runtime.run().expect("rejected action call should surface");
    assert!(matches!(
        emitted_effect.effect,
        TestEffect::CoreSet(
            waymark_vm_interpreter_coreset::Effect::UnhandledException(Exception {
                type_id,
                details: ReadyValue::String(details),
            })
        ) if type_id == "ValueError" && details == "boom"
    ));
}
