//! `CoreSet::Select` suspends the runtime on several promises at once and
//! resumes through the arm of whichever settles first.

use waymark_vm_instructions_coreset::{CoreSet, SelectArm};
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{
    Instruction, TestActionRef, TestReadyValue, new_runtime, new_runtime_with_args,
};

fn action_call_promise_state_id(
    effect: Effect<
        waymark_vm_interpreter_coreset::Effect<TestReadyValue>,
        waymark_vm_interpreter_extcallset::Effect<TestActionRef, TestReadyValue>,
        core::convert::Infallible,
    >,
) -> waymark_vm_runtime_promise_core::PromiseStateId {
    match effect {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            ..
        }) => promise_state_id,
        other => panic!("expected an action call effect: {other:?}"),
    }
}

fn completed_value(
    effect: Effect<
        waymark_vm_interpreter_coreset::Effect<TestReadyValue>,
        waymark_vm_interpreter_extcallset::Effect<TestActionRef, TestReadyValue>,
        core::convert::Infallible,
    >,
) -> TestReadyValue {
    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => value,
        other => panic!("expected the program to complete: {other:?}"),
    }
}

/// The executable shared by the pending-source tests: two action calls
/// selected over, with each arm returning its own delivery register.
fn two_action_select_executable() -> waymark_vm_bytecode::Executable<Instruction> {
    executable(vec![function::<Instruction>(
        4,
        vec![
            vec![
                ExtCallSet::ActionCall {
                    dst: RegisterId(0),
                    action_ref: TestActionRef(1),
                    args: Vec::new(),
                    resume: StateId(1),
                }
                .into(),
            ],
            vec![
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: TestActionRef(2),
                    args: Vec::new(),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![
                CoreSet::Select {
                    arms: vec![
                        SelectArm {
                            src: RegisterId(0),
                            dst: RegisterId(2),
                            resume: StateId(3),
                        },
                        SelectArm {
                            src: RegisterId(1),
                            dst: RegisterId(3),
                            resume: StateId(4),
                        },
                    ],
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(2) }.into()],
            vec![CoreSet::Return { src: RegisterId(3) }.into()],
        ],
    )])
}

/// The first settled arm resumes the frame at its own state with its own
/// delivery register - and the losing arm is inert.
#[test]
fn select_over_pending_sources_resumes_through_the_first_settled_arm() {
    let mut runtime = new_runtime(two_action_select_executable());

    let first_effect = runtime.run().expect("first action call should emit");
    let first_promise = action_call_promise_state_id(first_effect.effect);

    let second_effect = runtime.run().expect("second action call should emit");
    let second_promise = action_call_promise_state_id(second_effect.effect);

    assert!(
        matches!(runtime.run(), Err(RunError::NoReadyFrame)),
        "the frame should suspend selecting over the sources"
    );

    runtime
        .resolve_promise(second_promise, TestReadyValue::Int(99))
        .expect("second source should resolve cleanly");

    let emitted_effect = runtime.run().expect("taken arm should complete the run");
    assert_eq!(
        completed_value(emitted_effect.effect),
        TestReadyValue::Int(99)
    );

    // The losing source settles later and its claim is inert.
    runtime
        .resolve_promise(first_promise, TestReadyValue::Int(7))
        .expect("late loser settlement should be accepted and inert");
}

/// A rejected source takes its arm by raising at the arm's resume state.
#[test]
fn select_delivers_rejections_by_raising_at_the_arm() {
    let mut runtime = new_runtime(two_action_select_executable());

    let first_effect = runtime.run().expect("first action call should emit");
    let first_promise = action_call_promise_state_id(first_effect.effect);

    let _second_effect = runtime.run().expect("second action call should emit");

    assert!(
        matches!(runtime.run(), Err(RunError::NoReadyFrame)),
        "the frame should suspend selecting over the sources"
    );

    runtime
        .reject_promise(
            first_promise,
            Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(41),
            },
        )
        .expect("first source should reject cleanly");

    let emitted_effect = runtime
        .run()
        .expect("the raised arm should surface the exception");
    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::UnhandledException(exception)) => {
            assert_eq!(exception.type_id, "ValueError");
        }
        other => panic!("expected an unhandled exception: {other:?}"),
    }
}

/// A source whose promise has already settled is taken at scan time -
/// the select never suspends and no claims are planted.
#[test]
fn select_with_an_already_settled_source_is_taken_at_scan_time() {
    let mut runtime = new_runtime(two_action_select_executable());

    let first_effect = runtime.run().expect("first action call should emit");
    let _first_promise = action_call_promise_state_id(first_effect.effect);

    let second_effect = runtime.run().expect("second action call should emit");
    let second_promise = action_call_promise_state_id(second_effect.effect);

    // Settle the second source before the select instruction runs.
    runtime
        .resolve_promise(second_promise, TestReadyValue::Int(99))
        .expect("second source should resolve cleanly");

    let emitted_effect = runtime
        .run()
        .expect("select should take the settled arm at scan time");
    assert_eq!(
        completed_value(emitted_effect.effect),
        TestReadyValue::Int(99)
    );
}

/// A source register holding a plain ready value - not a promise at all -
/// is taken at scan time.
#[test]
fn select_with_a_ready_value_source_is_taken_at_scan_time() {
    let executable = executable(vec![function::<Instruction>(
        4,
        vec![
            vec![
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: TestActionRef(1),
                    args: Vec::new(),
                    resume: StateId(1),
                }
                .into(),
            ],
            vec![
                // Register 0 holds the ready function argument.
                CoreSet::Select {
                    arms: vec![
                        SelectArm {
                            src: RegisterId(1),
                            dst: RegisterId(2),
                            resume: StateId(2),
                        },
                        SelectArm {
                            src: RegisterId(0),
                            dst: RegisterId(3),
                            resume: StateId(3),
                        },
                    ],
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(2) }.into()],
            vec![CoreSet::Return { src: RegisterId(3) }.into()],
        ],
    )]);

    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(5)]);

    let action_effect = runtime.run().expect("the action call should emit");
    let _action_promise = action_call_promise_state_id(action_effect.effect);

    let emitted_effect = runtime
        .run()
        .expect("select should take the ready arm at scan time");
    assert_eq!(
        completed_value(emitted_effect.effect),
        TestReadyValue::Int(5)
    );
}

/// A select with no arms is a bytecode mistake and fails the run.
#[test]
fn select_with_no_arms_fails_the_run() {
    let executable = executable(vec![function::<Instruction>(
        1,
        vec![vec![
            CoreSet::Select { arms: Vec::new() }.into(),
            CoreSet::Return { src: RegisterId(0) }.into(),
        ]],
    )]);

    let mut runtime = new_runtime(executable);

    let err = runtime
        .run()
        .expect_err("a select with no arms should fail the run");

    assert!(
        matches!(
            &err,
            RunError::Step(waymark_vm_runtime::step::Error::Execution(
                waymark_vm_interpreter_fullset::Error::CoreSet(
                    waymark_vm_interpreter_coreset::Error::Select(
                        waymark_vm_interpreter_coreset::SelectError::EmptyArms
                    )
                )
            ))
        ),
        "unexpected error: {err:?}"
    );
}
