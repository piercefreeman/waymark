//! Raising exception values from registers.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::{Effect, Error, RaiseError};
use waymark_vm_runtime::{RunError, step};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{Instruction, TestReadyValue, TestSpec, TestValue, new_runtime_with_args};

#[test]
fn runtime_raises_exception_values_from_registers() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![vec![CoreSet::Raise { src: RegisterId(0) }]],
    )]);
    let raised = TestReadyValue::Exception(Box::new(Exception {
        type_id: "ValueError".to_owned(),
        details: TestValue::Ready(TestReadyValue::Int(7)),
    }));

    let mut runtime = new_runtime_with_args(executable, vec![raised]);

    let emitted_effect = runtime
        .run()
        .expect("raise program should emit an unhandled exception");

    match emitted_effect.effect {
        Effect::UnhandledException(Exception { type_id, details }) => {
            assert_eq!(type_id, "ValueError");
            assert_eq!(details, TestReadyValue::Int(7));
        }
        Effect::Complete(value) => {
            panic!("raise program should not complete successfully: {value:?}")
        }
    }
}

#[test]
fn raise_rejects_non_exception_values() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![vec![CoreSet::Raise { src: RegisterId(0) }]],
    )]);

    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(7)]);

    let result = runtime.run();

    assert!(matches!(
        result,
        Err(RunError::Step(step::Error::Execution(
            Error::<TestSpec>::Raise(RaiseError::SourceNotException,)
        )))
    ));
}

#[test]
fn bubble_exception_handles_same_frame_exceptions_with_local_handlers() {
    let function = function::<Instruction>(
        2,
        vec![
            vec![
                CoreSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(1),
                        exception_types: vec!["ValueError".to_owned()],
                        exception_dst: Some(RegisterId(1)),
                    }],
                },
                CoreSet::Raise { src: RegisterId(0) },
            ],
            vec![CoreSet::Return { src: RegisterId(1) }],
        ],
    );
    let executable = executable(vec![function]);
    let raised = TestReadyValue::Exception(Box::new(Exception {
        type_id: "ValueError".to_owned(),
        details: TestValue::Ready(TestReadyValue::Int(7)),
    }));

    let mut runtime = new_runtime_with_args(executable, vec![raised]);

    let emitted_effect = runtime
        .run()
        .expect("local handler should catch the raised exception");

    match emitted_effect.effect {
        Effect::Complete(TestReadyValue::Exception(exception)) => {
            assert_eq!(exception.type_id, "ValueError");
            assert_eq!(exception.details, TestValue::Ready(TestReadyValue::Int(7)));
        }
        other => panic!("local handler should return the captured exception: {other:?}"),
    }
}
