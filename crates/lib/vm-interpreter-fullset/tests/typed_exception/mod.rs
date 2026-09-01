//! Pureset operation errors raised as catchable typed exceptions.
//!
//! A raising pureset instruction records the exception on the frame and
//! continues; the coreset `after_execute` hook then bubbles it — into
//! a handler when one matches, or up to an `UnhandledException` effect
//! otherwise.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_pureset::{BinaryOp, BinaryOpKind, PureSet, UnaryOp, UnaryOpKind};
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{Instruction, TestConstValue, TestReadyValue, TestValue, new_runtime};

/// A straight-line prologue that raises a `TypeError` from the pureset
/// interpreter by adding an integer to a boolean.
fn raising_prologue() -> Vec<Instruction> {
    vec![
        PureSet::LoadConst {
            dst: RegisterId(0),
            value: TestConstValue::Int(0),
        }
        .into(),
        PureSet::Unary {
            kind: UnaryOpKind::Not,
            op: UnaryOp {
                dst: RegisterId(1),
                src: RegisterId(0),
            },
        }
        .into(),
        PureSet::Binary {
            kind: BinaryOpKind::Add,
            op: BinaryOp {
                dst: RegisterId(0),
                a: RegisterId(0),
                b: RegisterId(1),
            },
        }
        .into(),
        // Only reached if the raise fails to bubble; completes with a value
        // the assertions below reject.
        CoreSet::Return { src: RegisterId(0) }.into(),
    ]
}

fn expected_exception() -> Exception<TestValue> {
    Exception {
        type_id: "TypeError".to_owned(),
        details: TestValue::Ready(TestReadyValue::Text(
            "+ is not supported for these operands".to_owned(),
        )),
    }
}

#[test]
fn raised_typed_exceptions_bubble_into_matching_handlers() {
    let mut instructions = vec![
        CoreSet::PushExceptionHandlers {
            handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                handler_state: StateId(1),
                exception_types: vec!["TypeError".to_owned()],
                exception_dst: Some(RegisterId(2)),
            }],
            finally_state: None,
        }
        .into(),
    ];
    instructions.extend(raising_prologue());

    let executable = executable(vec![function::<Instruction>(
        3,
        vec![
            instructions,
            vec![CoreSet::Return { src: RegisterId(2) }.into()],
        ],
    )]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("caught typed exception should complete through the handler");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(
                value,
                TestReadyValue::Exception(Box::new(expected_exception()))
            );
        }
        effect => {
            panic!("caught typed exception should complete with the bound exception: {effect:?}")
        }
    }
}

#[test]
fn typed_exceptions_raised_in_called_functions_are_caught_by_local_handlers() {
    let mut callee_instructions = vec![
        CoreSet::PushExceptionHandlers {
            handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                handler_state: StateId(1),
                exception_types: vec!["TypeError".to_owned()],
                exception_dst: Some(RegisterId(2)),
            }],
            finally_state: None,
        }
        .into(),
    ];
    callee_instructions.extend(raising_prologue());

    let executable = executable(vec![
        function::<Instruction>(
            2,
            vec![
                vec![
                    CoreSet::Call {
                        dst: RegisterId(1),
                        function_id: waymark_vm_runtime_test::FunctionId(1),
                        args: vec![],
                    }
                    .into(),
                    CoreSet::Await {
                        dst: RegisterId(0),
                        src: RegisterId(1),
                        resume: StateId(1),
                    }
                    .into(),
                ],
                vec![CoreSet::Return { src: RegisterId(0) }.into()],
            ],
        ),
        function::<Instruction>(
            3,
            vec![
                callee_instructions,
                vec![CoreSet::Return { src: RegisterId(2) }.into()],
            ],
        ),
    ]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("callee-caught typed exception should complete through the call");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(
                value,
                TestReadyValue::Exception(Box::new(expected_exception()))
            );
        }
        effect => {
            panic!(
                "callee-caught typed exception should complete with the bound exception: {effect:?}"
            )
        }
    }
}

#[test]
fn typed_exceptions_raised_in_called_functions_propagate_to_caller_handlers() {
    let executable = executable(vec![
        function::<Instruction>(
            3,
            vec![
                vec![
                    CoreSet::PushExceptionHandlers {
                        handlers: vec![waymark_vm_exception_handler::ExceptionHandler {
                            handler_state: StateId(2),
                            exception_types: vec!["TypeError".to_owned()],
                            exception_dst: Some(RegisterId(2)),
                        }],
                        finally_state: None,
                    }
                    .into(),
                    CoreSet::Call {
                        dst: RegisterId(1),
                        function_id: waymark_vm_runtime_test::FunctionId(1),
                        args: vec![],
                    }
                    .into(),
                    CoreSet::Await {
                        dst: RegisterId(0),
                        src: RegisterId(1),
                        resume: StateId(1),
                    }
                    .into(),
                ],
                // Only reached if the rejection fails to raise in the caller.
                vec![CoreSet::Return { src: RegisterId(0) }.into()],
                vec![CoreSet::Return { src: RegisterId(2) }.into()],
            ],
        ),
        function::<Instruction>(2, vec![raising_prologue()]),
    ]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("caller-caught typed exception should complete through the handler");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(
                value,
                TestReadyValue::Exception(Box::new(expected_exception()))
            );
        }
        effect => {
            panic!(
                "caller-caught typed exception should complete with the bound exception: {effect:?}"
            )
        }
    }
}

#[test]
fn uncaught_typed_exceptions_surface_as_unhandled_exceptions() {
    let executable = executable(vec![function::<Instruction>(2, vec![raising_prologue()])]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("uncaught typed exception should emit an unhandled exception");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception.type_id, "TypeError");
            assert_eq!(
                exception.details,
                TestReadyValue::Text("+ is not supported for these operands".to_owned())
            );
        }
        effect => panic!("uncaught typed exception should not complete: {effect:?}"),
    }
}
