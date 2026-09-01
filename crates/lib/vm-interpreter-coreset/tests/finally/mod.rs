//! VM-managed finalizer execution.
//!
//! These fixtures use bytecode directly because they isolate the coreset VM's
//! unwind contract from compiler lowering. Compiler integration tests cover the
//! same control-flow paths from parsed IR.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{Instruction, TestReadyValue, TestValue, new_runtime_with_args};

#[test]
fn runtime_unwinds_nested_finalizers_before_resuming() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![
            vec![
                CoreSet::PushExceptionHandlers {
                    handlers: Vec::new(),
                    finally_state: Some(StateId(2)),
                },
                CoreSet::PushExceptionHandlers {
                    handlers: Vec::new(),
                    finally_state: Some(StateId(1)),
                },
                CoreSet::Unwind {
                    depth: 0,
                    target_state: StateId(3),
                },
            ],
            vec![CoreSet::ContinueUnwind],
            vec![CoreSet::ContinueUnwind],
            vec![CoreSet::Return { src: RegisterId(0) }],
        ],
    )]);
    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(9)]);

    let emitted_effect = runtime.run().expect("nested finalizers should complete");

    match emitted_effect.effect {
        Effect::Complete(value) => assert_eq!(value, TestReadyValue::Int(9)),
        Effect::UnhandledException(exception) => {
            panic!("nested finalizers should not raise an exception: {exception:?}")
        }
    }
}

#[test]
fn matching_handler_runs_its_registered_finalizer() {
    let executable = executable(vec![function::<Instruction>(
        3,
        vec![
            vec![
                CoreSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(1),
                        exception_types: vec!["ValueError".to_owned()],
                        exception_dst: Some(RegisterId(2)),
                    }],
                    finally_state: Some(StateId(2)),
                },
                CoreSet::Raise { src: RegisterId(0) },
            ],
            vec![CoreSet::Unwind {
                depth: 0,
                target_state: StateId(3),
            }],
            vec![CoreSet::Return { src: RegisterId(1) }],
            vec![CoreSet::Return { src: RegisterId(2) }],
        ],
    )]);
    let raised = TestReadyValue::Exception(Box::new(Exception {
        type_id: "ValueError".to_owned(),
        details: TestValue::Ready(TestReadyValue::Int(7)),
    }));
    let mut runtime = new_runtime_with_args(executable, vec![raised, TestReadyValue::Int(9)]);

    let emitted_effect = runtime.run().expect("registered finalizer should return");

    match emitted_effect.effect {
        Effect::Complete(value) => assert_eq!(value, TestReadyValue::Int(9)),
        other => panic!("registered finalizer should override the handler: {other:?}"),
    }
}

#[test]
fn exception_from_finalizer_replaces_its_pending_jump() {
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
                    finally_state: None,
                },
                CoreSet::PushExceptionHandlers {
                    handlers: Vec::new(),
                    finally_state: Some(StateId(1)),
                },
                CoreSet::Unwind {
                    depth: 1,
                    target_state: StateId(3),
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
