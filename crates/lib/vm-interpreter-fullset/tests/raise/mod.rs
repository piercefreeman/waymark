//! Raising: exception values from registers, exceptions embedded in the
//! bytecode, and the handler patterns that catch them.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_interpreter_excset::RaiseError;
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime::{RunError, step};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{
    Instruction, TestConstException, TestConstValue, TestException, TestExceptionPattern,
    TestReadyValue, TestValue, new_runtime, new_runtime_with_args,
};

fn value_error(details: i64) -> TestException {
    TestException {
        type_id: "ValueError".to_owned(),
        details: TestValue::Ready(TestReadyValue::Int(details)),
    }
}

#[test]
fn runtime_raises_exception_values_from_registers() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![vec![ExcSet::Raise { src: RegisterId(0) }.into()]],
    )]);
    let raised = TestReadyValue::Exception(Box::new(value_error(7)));

    let mut runtime = new_runtime_with_args(executable, vec![raised]);

    let emitted_effect = runtime
        .run()
        .expect("raise program should emit an unhandled exception");

    match emitted_effect.effect {
        Effect::ExcSet(waymark_vm_interpreter_excset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception, value_error(7));
        }
        effect => panic!("raise program should not complete successfully: {effect:?}"),
    }
}

#[test]
fn raise_rejects_non_exception_values() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![vec![ExcSet::Raise { src: RegisterId(0) }.into()]],
    )]);

    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(7)]);

    let result = runtime.run();

    assert!(matches!(
        result,
        Err(RunError::Step(step::Error::Execution(
            waymark_vm_interpreter_fullset::Error::ExcSet(
                waymark_vm_interpreter_excset::Error::Raise(RaiseError::Value(_))
            )
        )))
    ));
}

#[test]
fn runtime_raises_exceptions_embedded_in_the_bytecode() {
    let executable = executable(vec![function::<Instruction>(
        0,
        vec![vec![
            ExcSet::RaiseConst {
                exception: TestConstException {
                    type_id: "ValueError",
                    details: TestConstValue::Int(7),
                },
            }
            .into(),
        ]],
    )]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("raise-const program should emit an unhandled exception");

    match emitted_effect.effect {
        Effect::ExcSet(waymark_vm_interpreter_excset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception, value_error(7));
        }
        effect => panic!("raise-const program should not complete successfully: {effect:?}"),
    }
}

#[test]
fn same_frame_exceptions_unwind_into_local_handlers() {
    let function = function::<Instruction>(
        2,
        vec![
            vec![
                ExcSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(1),
                        pattern: TestExceptionPattern::type_id("ValueError"),
                        exception_dst: Some(RegisterId(1)),
                    }],
                }
                .into(),
                ExcSet::Raise { src: RegisterId(0) }.into(),
            ],
            vec![CoreSet::Return { src: RegisterId(1) }.into()],
        ],
    );
    let executable = executable(vec![function]);
    let raised = TestReadyValue::Exception(Box::new(value_error(7)));

    let mut runtime = new_runtime_with_args(executable, vec![raised]);

    let emitted_effect = runtime
        .run()
        .expect("local handler should catch the raised exception");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestReadyValue::Exception(Box::new(value_error(7))));
        }
        effect => panic!("local handler should return the captured exception: {effect:?}"),
    }
}

#[test]
fn handlers_listing_another_pattern_do_not_catch() {
    let function = function::<Instruction>(
        2,
        vec![
            vec![
                ExcSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(1),
                        pattern: TestExceptionPattern::type_id("TypeError"),
                        exception_dst: Some(RegisterId(1)),
                    }],
                }
                .into(),
                ExcSet::Raise { src: RegisterId(0) }.into(),
            ],
            vec![CoreSet::Return { src: RegisterId(1) }.into()],
        ],
    );
    let executable = executable(vec![function]);
    let raised = TestReadyValue::Exception(Box::new(value_error(7)));

    let mut runtime = new_runtime_with_args(executable, vec![raised]);

    let emitted_effect = runtime
        .run()
        .expect("an unmatched raise should emit an unhandled exception");

    match emitted_effect.effect {
        Effect::ExcSet(waymark_vm_interpreter_excset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception, value_error(7));
        }
        effect => panic!("an unmatched raise should not complete: {effect:?}"),
    }
}

#[test]
fn catch_all_handlers_catch_without_a_destination() {
    let function = function::<Instruction>(
        1,
        vec![
            vec![
                ExcSet::PushExceptionHandlers {
                    handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                        handler_state: StateId(1),
                        pattern: TestExceptionPattern::Any,
                        exception_dst: None,
                    }],
                }
                .into(),
                ExcSet::Raise { src: RegisterId(0) }.into(),
            ],
            vec![CoreSet::Return { src: RegisterId(0) }.into()],
        ],
    );
    let executable = executable(vec![function]);
    let raised = TestReadyValue::Exception(Box::new(value_error(7)));

    let mut runtime = new_runtime_with_args(executable, vec![raised]);

    let emitted_effect = runtime
        .run()
        .expect("the catch-all handler should catch the raised exception");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            // The register the raise read from is untouched: nothing was
            // bound.
            assert_eq!(value, TestReadyValue::Exception(Box::new(value_error(7))));
        }
        effect => panic!("the catch-all handler should complete: {effect:?}"),
    }
}
