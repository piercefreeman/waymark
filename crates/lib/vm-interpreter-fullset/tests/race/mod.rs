//! `CoreSet::Race` creates a race promise that resolves with the index of
//! the first source to settle.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{
    Instruction, TestActionRef, TestReadyValue, new_runtime, new_runtime_with_args,
};

fn action_call_promise_state_id(
    effect: Effect<TestReadyValue, TestActionRef, TestReadyValue>,
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
    effect: Effect<TestReadyValue, TestActionRef, TestReadyValue>,
) -> TestReadyValue {
    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => value,
        other => panic!("expected the program to complete: {other:?}"),
    }
}

/// Two pending action calls raced: the race promise stays pending until
/// a source settles, then resolves with the settled arm's index.
#[test]
fn race_of_pending_sources_resolves_with_the_first_settled_arm_index() {
    let executable = executable(vec![function::<Instruction>(
        5,
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
                CoreSet::Race {
                    dst: RegisterId(2),
                    srcs: vec![RegisterId(0), RegisterId(1)],
                }
                .into(),
                CoreSet::Await {
                    dst: RegisterId(3),
                    src: RegisterId(2),
                    resume: StateId(3),
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(3) }.into()],
        ],
    )]);

    let mut runtime = new_runtime(executable);

    let first_effect = runtime.run().expect("first action call should emit");
    let first_promise = action_call_promise_state_id(first_effect.effect);

    let second_effect = runtime.run().expect("second action call should emit");
    let second_promise = action_call_promise_state_id(second_effect.effect);

    assert!(
        matches!(runtime.run(), Err(RunError::NoReadyFrame)),
        "the frame should suspend awaiting the race promise"
    );

    runtime
        .resolve_promise(second_promise, TestReadyValue::Int(99))
        .expect("second source should resolve cleanly");

    let emitted_effect = runtime.run().expect("race winner should complete the run");
    assert_eq!(
        completed_value(emitted_effect.effect),
        TestReadyValue::Int(1)
    );

    // The losing source settles later and its race arm is inert.
    runtime
        .resolve_promise(first_promise, TestReadyValue::Int(7))
        .expect("late loser settlement should be accepted and inert");
}

/// A source whose promise has already settled wins at scan time - the
/// race never suspends and no race promise is allocated.
#[test]
fn race_with_an_already_settled_source_wins_at_scan_time() {
    let executable = executable(vec![function::<Instruction>(
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
                // The promise in register 1 settles before the race
                // instruction runs; the still-pending action call in
                // register 0 is listed first but loses the scan.
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: TestActionRef(2),
                    args: Vec::new(),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![
                CoreSet::Race {
                    dst: RegisterId(2),
                    srcs: vec![RegisterId(0), RegisterId(1)],
                }
                .into(),
                CoreSet::Await {
                    dst: RegisterId(3),
                    src: RegisterId(2),
                    resume: StateId(3),
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(3) }.into()],
        ],
    )]);

    let mut runtime = new_runtime(executable);

    let first_effect = runtime.run().expect("first action call should emit");
    let _first_promise = action_call_promise_state_id(first_effect.effect);

    let second_effect = runtime.run().expect("second action call should emit");
    let second_promise = action_call_promise_state_id(second_effect.effect);

    // Settle the second source before the race instruction runs.
    runtime
        .resolve_promise(second_promise, TestReadyValue::Int(99))
        .expect("second source should resolve cleanly");

    let emitted_effect = runtime
        .run()
        .expect("race should win at scan time and complete without suspending");
    assert_eq!(
        completed_value(emitted_effect.effect),
        TestReadyValue::Int(1)
    );
}

/// A source register holding a plain ready value - not a promise at all -
/// counts as settled and wins at scan time.
#[test]
fn race_with_a_ready_value_source_wins_at_scan_time() {
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
                CoreSet::Race {
                    dst: RegisterId(2),
                    srcs: vec![RegisterId(1), RegisterId(0)],
                }
                .into(),
                CoreSet::Await {
                    dst: RegisterId(3),
                    src: RegisterId(2),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![CoreSet::Return { src: RegisterId(3) }.into()],
        ],
    )]);

    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(5)]);

    let action_effect = runtime.run().expect("the action call should emit");
    let _action_promise = action_call_promise_state_id(action_effect.effect);

    let emitted_effect = runtime
        .run()
        .expect("race should win at scan time and complete without suspending");
    assert_eq!(
        completed_value(emitted_effect.effect),
        TestReadyValue::Int(1)
    );
}

/// A race with no sources is a bytecode mistake and fails the run.
#[test]
fn race_with_no_sources_fails_the_run() {
    let executable = executable(vec![function::<Instruction>(
        2,
        vec![vec![
            CoreSet::Race {
                dst: RegisterId(0),
                srcs: Vec::new(),
            }
            .into(),
            CoreSet::Return { src: RegisterId(0) }.into(),
        ]],
    )]);

    let mut runtime = new_runtime(executable);

    let err = runtime
        .run()
        .expect_err("a race with no sources should fail the run");

    assert!(
        matches!(
            &err,
            RunError::Step(waymark_vm_runtime::step::Error::Execution(
                waymark_vm_interpreter_fullset::Error::CoreSet(
                    waymark_vm_interpreter_coreset::Error::Race(
                        waymark_vm_interpreter_coreset::RaceError::EmptySources
                    )
                )
            ))
        ),
        "unexpected error: {err:?}"
    );
}
