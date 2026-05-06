mod support;

use support::{TestExtCallId, TestValue, compile_program, runtime};
use waymark_vm_ast_old_helpers::*;
use waymark_vm_interpreter_fullset::Effect;

#[test]
fn compiles_assignments_and_addition_to_completion() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("x", int(2)),
            assignment("y", int(3)),
            return_stmt(Some(add(variable("x"), variable("y")))),
        ],
    )]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let effect = runtime.run().expect("program should complete");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 5),
        other => panic!("unexpected runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_user_function_calls() {
    let program = program(vec![
        function(
            "main",
            &[],
            vec![return_stmt(Some(function_expr("increment", vec![int(41)])))],
        ),
        function(
            "increment",
            &["value"],
            vec![return_stmt(Some(add(variable("value"), int(1))))],
        ),
    ]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let effect = runtime.run().expect("program should complete");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 42),
        other => panic!("unexpected runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_action_calls_into_extcalls() {
    let program = program(vec![function(
        "main",
        &[],
        vec![return_stmt(Some(action_expr(
            "fetch",
            vec![("value", int(41))],
        )))],
    )]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let effect = runtime.run().expect("program should emit an extcall");

    let promise_state_id = match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::ExtCall {
            promise_state_id,
            extcall_id,
            args,
        }) => {
            assert_eq!(extcall_id, TestExtCallId("fetch".to_owned()));
            assert_eq!(args, vec![TestValue::Int(41)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(42))
        .expect("extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolution");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 42),
        other => panic!("unexpected second runtime effect: {other:?}"),
    }
}
