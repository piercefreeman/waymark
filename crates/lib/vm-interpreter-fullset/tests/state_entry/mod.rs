//! State-entry behavior for the fullset interpreter.

use waymark_vm_bytecode::Executable;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_interpreter_fullset::FullSetInterpreter;
use waymark_vm_runtime::Runtime;
use waymark_vm_runtime_core::{CaptureRuntimeView, FullRuntimeView, RegisterId};
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{FunctionId, StateId, executable, function};

use crate::support::{Instruction, TestActionRef, TestReadyValue, TestSpec, TestValue};

#[derive(Default)]
struct NoPendingExceptionExecuteInterpreter {
    inner: FullSetInterpreter<TestSpec, Executable<Instruction>, TestValue>,
}

impl CaptureRuntimeView<Executable<Instruction>, FunctionId, StateId, TestValue>
    for NoPendingExceptionExecuteInterpreter
{
    type RuntimeView<'r> =
        FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, TestValue>;

    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, TestValue>,
    ) -> Self::RuntimeView<'r> {
        view
    }
}

impl waymark_vm_interpreter::Interpreter for NoPendingExceptionExecuteInterpreter {
    type RuntimeView<'r> =
        FullRuntimeView<'r, Executable<Instruction>, FunctionId, StateId, TestValue>;
    type Frame = waymark_vm_runtime::FrameFor<Executable<Instruction>, TestValue>;
    type Instruction = Instruction;
    type Error = <FullSetInterpreter<TestSpec, Executable<Instruction>, TestValue> as waymark_vm_interpreter::Interpreter>::Error;
    type Effect = <FullSetInterpreter<TestSpec, Executable<Instruction>, TestValue> as waymark_vm_interpreter::Interpreter>::Effect;

    fn enter_state<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: &mut Self::Frame,
    ) -> Result<waymark_vm_interpreter::StateEntryOutcome<Self::Effect>, Self::Error> {
        let outcome =
            waymark_vm_interpreter::Interpreter::enter_state(&self.inner, runtime_view, frame)?;
        Ok(outcome)
    }

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        if frame.exception.is_some() {
            panic!("execute should not receive frames with pending exceptions");
        }

        let outcome = waymark_vm_interpreter::Interpreter::execute(
            &self.inner,
            runtime_view,
            frame,
            instruction,
        )?;
        Ok(outcome)
    }
}

#[test]
fn pending_exceptions_are_consumed_before_execute_dispatch() {
    let executable = executable(vec![function::<Instruction>(
        2,
        vec![
            vec![
                ExtCallSet::ActionCall {
                    dst: RegisterId(0),
                    action_ref: TestActionRef(7),
                    args: vec![],
                    resume: StateId(1),
                }
                .into(),
            ],
            vec![
                CoreSet::Await {
                    dst: RegisterId(1),
                    src: RegisterId(0),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(1) }.into()],
        ],
    )]);

    let mut runtime = Runtime::with_conventional_entrypoint(
        NoPendingExceptionExecuteInterpreter::default(),
        executable,
    )
    .expect("function 0 should exist");

    let waymark_vm_interpreter_fullset::Effect::ExtCallSet(
        waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id, ..
        },
    ) = runtime
        .run()
        .expect("first run should suspend on the action call")
    else {
        panic!("first run should emit an action call");
    };

    runtime
        .reject_promise(
            promise_state_id,
            Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(7),
            },
        )
        .expect("action-call promise should resolve exceptionally");

    let effect = runtime
        .run()
        .expect("state entry should bubble the pending exception before execute dispatch");

    match effect {
        waymark_vm_interpreter_fullset::Effect::CoreSet(
            waymark_vm_interpreter_coreset::Effect::UnhandledException(Exception {
                type_id,
                details,
            }),
        ) => {
            assert_eq!(type_id, "ValueError");
            assert_eq!(details, TestReadyValue::Int(7));
        }
        other => panic!(
            "program should surface the unhandled exception without dispatching execute: {other:?}"
        ),
    }
}
