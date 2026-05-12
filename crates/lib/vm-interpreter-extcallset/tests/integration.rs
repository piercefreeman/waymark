use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_extcallset::{Effect, ExtCallSetInterpreter, RuntimeView};
use waymark_vm_runtime::{CallSpec, Runtime};
use waymark_vm_runtime_core::{
    CaptureRuntimeView, Frame, FullRuntimeView, Promise, PromiseState, PromiseStateId, RegisterId,
};
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

#[derive(Debug)]
struct TestSpec;

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = RegisterId;
    type StateId = StateId;
    type ExtCallId = usize;
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
    ExtCall(Effect<i32, usize>),
    PendingPromiseStateId(PromiseStateId),
}

#[derive(Default)]
struct RuntimeInterpreter {
    extcall: ExtCallSetInterpreter<TestSpec, FunctionId, StateId, i32>,
}

impl<Executable> CaptureRuntimeView<Executable, FunctionId, StateId, i32> for RuntimeInterpreter {
    type RuntimeView<'r>
        = RuntimeView<'r, FunctionId, StateId, i32>
    where
        Executable: 'r,
        FunctionId: 'r,
        StateId: 'r,
        i32: 'r;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, Executable, FunctionId, StateId, i32>,
    ) -> Self::RuntimeView<'r> {
        let FullRuntimeView { state, .. } = view;
        RuntimeView { state }
    }
}

impl waymark_vm_interpreter::Interpreter for RuntimeInterpreter {
    type RuntimeView<'r> = RuntimeView<'r, FunctionId, StateId, i32>;
    type Frame = Frame<FunctionId, StateId, Promise<i32>>;
    type Instruction = RuntimeInstruction;
    type Error = waymark_vm_interpreter_extcallset::Error;
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
                .map(|outcome| outcome.map_effect(TestEffect::ExtCall))
            }
            RuntimeInstruction::InspectPending(register) => {
                let RuntimeView { state } = runtime_view;

                let Promise::Pending(promise_state_id) = frame.regs[*register].clone() else {
                    panic!("register should hold the extcall's pending promise");
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
fn runtime_emits_an_extcall_effect_and_queues_the_resumed_frame() {
    let mut runtime = Runtime::with_custom_entrypoint(
        RuntimeInterpreter::default(),
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![
                vec![
                    ExtCallSet::ExtCall {
                        dst: RegisterId(1),
                        extcall_id: 7,
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
            args: vec![41],
        },
    )
    .expect("function 0 should exist");

    let TestEffect::ExtCall(Effect::ExtCall {
        promise_state_id,
        extcall_id,
        args,
    }) = runtime.run().expect("first run should emit the extcall")
    else {
        panic!("first run should emit an extcall effect");
    };

    assert_eq!(extcall_id, 7);
    assert_eq!(args, vec![41]);

    let TestEffect::PendingPromiseStateId(resumed_promise_state_id) = runtime
        .run()
        .expect("second run should execute the resumed frame")
    else {
        panic!("second run should inspect the resumed pending promise");
    };

    assert_eq!(resumed_promise_state_id, promise_state_id);
}
