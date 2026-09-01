//! Shared finalizer state execution.

use waymark_vm_instructions_coreset::{CoreSet, StateTarget};
use waymark_vm_interpreter_coreset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{Instruction, TestReadyValue, TestValue, new_runtime_with_args};

fn target(state: usize, unwind_depth: usize) -> StateTarget<StateId> {
    StateTarget {
        state: StateId(state),
        unwind_depth,
    }
}

#[test]
fn runtime_resumes_nested_finalizer_calls() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![
            vec![CoreSet::CallStates {
                targets: vec![target(1, 0), target(3, 0)],
                return_to: target(4, 0),
            }],
            vec![CoreSet::CallStates {
                targets: vec![target(2, 1)],
                return_to: target(5, 1),
            }],
            vec![CoreSet::ReturnState],
            vec![CoreSet::ReturnState],
            vec![CoreSet::Return { src: RegisterId(0) }],
            vec![CoreSet::ReturnState],
        ],
    )]);
    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(9)]);

    let emitted_effect = runtime.run().expect("finalizer calls should complete");

    match emitted_effect.effect {
        Effect::Complete(value) => assert_eq!(value, TestReadyValue::Int(9)),
        Effect::UnhandledException(exception) => {
            panic!("finalizer calls should not raise an exception: {exception:?}")
        }
    }
}

#[test]
fn exception_from_finalizer_replaces_its_pending_return() {
    let executable = executable(vec![function::<Instruction>(
        3,
        vec![
            vec![
                CoreSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(2),
                        exception_types: vec!["ValueError".to_owned()],
                        exception_dst: Some(RegisterId(2)),
                    }],
                },
                CoreSet::CallStates {
                    targets: vec![target(1, 1)],
                    return_to: target(3, 1),
                },
            ],
            vec![CoreSet::Raise { src: RegisterId(0) }],
            vec![CoreSet::Return { src: RegisterId(2) }],
            vec![CoreSet::Return { src: RegisterId(1) }],
        ],
    )]);
    let raised = TestReadyValue::Exception(Box::new(Exception {
        type_id: "ValueError".to_owned(),
        details: TestValue::Ready(TestReadyValue::Int(7)),
    }));
    let mut runtime = new_runtime_with_args(executable, vec![raised, TestReadyValue::Int(9)]);

    let emitted_effect = runtime
        .run()
        .expect("outer handler should catch the finalizer exception");

    match emitted_effect.effect {
        Effect::Complete(TestReadyValue::Exception(exception)) => {
            assert_eq!(exception.type_id, "ValueError");
        }
        other => panic!("handler should return the finalizer exception: {other:?}"),
    }
}
