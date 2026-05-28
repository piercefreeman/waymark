//! Integration test for resolving extcall action calls with exception values.

use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_coreset::CoreSetInterpreter;
use waymark_vm_interpreter_extcallset::ExtCallSetInterpreter;
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::{CaptureRuntimeView, Frame, FullRuntimeView, RegisterId};
use waymark_vm_runtime_exception::{AsException as _, Exception};
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};
use waymark_vm_value::{ReadyValue, Value};

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
    CoreSet(#[from] waymark_vm_interpreter_coreset::Error<TestSpec>),

    #[error(transparent)]
    ExtCallSet(#[from] waymark_vm_interpreter_extcallset::Error<Value>),
}

#[derive(Default)]
struct RuntimeInterpreter {
    core_set: CoreSetInterpreter<TestSpec, Executable<Instruction>, Value>,
    extcall_set: ExtCallSetInterpreter<TestSpec, FunctionId, StateId, Value>,
}

impl CaptureRuntimeView<Executable<Instruction>, FunctionId, StateId, Value>
    for RuntimeInterpreter
{
    type RuntimeView<'r>
        = FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>
    where
        Executable<Instruction>: 'r,
        FunctionId: 'r,
        StateId: 'r,
        Value: 'r;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>,
    ) -> Self::RuntimeView<'r> {
        view
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> = FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, Value>;
    type Frame = Frame<FunctionId, StateId, Value>;
    type Instruction = Instruction;
    type Error = TestError;
    type Effect = TestEffect;

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
                    Value,
                >::capture_runtime_view(runtime_view);
                self.core_set
                    .execute(runtime_view, frame, instruction)
                    .map(|outcome| outcome.map_effect(TestEffect::CoreSet))
                    .map_err(TestError::from)
            }
            Instruction::ExtCallSet(instruction) => {
                let runtime_view = ExtCallSetInterpreter::<TestSpec, FunctionId, StateId, Value>::capture_runtime_view(runtime_view);
                self.extcall_set
                    .execute(runtime_view, frame, instruction)
                    .map(|outcome| outcome.map_effect(TestEffect::ExtCallSet))
                    .map_err(TestError::from)
            }
        }
    }
}

#[test]
fn action_call_can_resume_with_an_exception_value() {
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

    let TestEffect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
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
    assert_eq!(args, vec![ReadyValue::Int(41)]);

    runtime
        .resolve_promise(
            promise_state_id,
            ReadyValue::Exception(Box::new(Exception {
                type_id: "ValueError".to_owned(),
                details: Value::Ready(ReadyValue::String("boom".to_owned())),
            })),
        )
        .expect("action call promise should resolve cleanly");

    let TestEffect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) = runtime
        .run()
        .expect("second run should complete with the exception value")
    else {
        panic!("second run should complete the resumed action call");
    };

    let exception = value
        .as_exception()
        .expect("completed value should remain an exception");
    assert_eq!(exception.type_id, "ValueError");
    assert_eq!(
        exception.details,
        Value::Ready(ReadyValue::String("boom".to_owned()))
    );
}
