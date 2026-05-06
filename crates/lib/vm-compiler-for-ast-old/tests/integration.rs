mod support;

use support::{TestExtCallId, TestValue, compile_program, runtime, runtime_with_args};
use waymark_vm_ast_old_helpers::{
    action_expr, add, assignment, break_stmt, conditional_stmt, continue_stmt, function,
    function_expr, int, program, return_stmt, variable, while_stmt,
};
use waymark_vm_bytecode_core::{FunctionId, InstructionId, StateId};
use waymark_vm_interpreter_fullset::Effect;

fn completed_int(effect: Effect<TestValue, TestExtCallId>) -> i64 {
    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => value,
        other => panic!("unexpected runtime effect: {other:?}"),
    }
}

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

#[test]
fn compiles_conditionals_assigning_on_all_paths() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                Vec::new(),
                Some(vec![assignment("x", int(2))]),
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let result = completed_int(
        runtime_with_args(compile_program(&program), vec![TestValue::Int(1)])
            .run()
            .expect("program should complete"),
    );
    assert_eq!(result, 1);

    let result = completed_int(
        runtime_with_args(compile_program(&program), vec![TestValue::Int(0)])
            .run()
            .expect("program should complete"),
    );
    assert_eq!(result, 2);
}

#[test]
fn compiles_terminal_conditionals_without_compiling_unreachable_tail() {
    let program = program(vec![function(
        "main",
        &["flag", "fallback"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![return_stmt(Some(int(1)))],
                vec![(variable("fallback"), vec![return_stmt(Some(int(2)))])],
                Some(vec![return_stmt(Some(int(3)))]),
            ),
            return_stmt(Some(variable("missing"))),
        ],
    )]);

    let result = completed_int(
        runtime_with_args(
            compile_program(&program),
            vec![TestValue::Int(1), TestValue::Int(0)],
        )
        .run()
        .expect("program should complete"),
    );
    assert_eq!(result, 1);

    let result = completed_int(
        runtime_with_args(
            compile_program(&program),
            vec![TestValue::Int(0), TestValue::Int(1)],
        )
        .run()
        .expect("program should complete"),
    );
    assert_eq!(result, 2);

    let result = completed_int(
        runtime_with_args(
            compile_program(&program),
            vec![TestValue::Int(0), TestValue::Int(0)],
        )
        .run()
        .expect("program should complete"),
    );
    assert_eq!(result, 3);
}

#[test]
fn compiles_while_loops_with_break() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            while_stmt(variable("flag"), vec![break_stmt()]),
            return_stmt(Some(int(7))),
        ],
    )]);

    let result = completed_int(
        runtime_with_args(compile_program(&program), vec![TestValue::Int(1)])
            .run()
            .expect("program should complete"),
    );
    assert_eq!(result, 7);

    let result = completed_int(
        runtime_with_args(compile_program(&program), vec![TestValue::Int(0)])
            .run()
            .expect("program should complete"),
    );
    assert_eq!(result, 7);
}

#[test]
fn lowers_continue_to_the_loop_condition_state() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            while_stmt(variable("flag"), vec![continue_stmt()]),
            return_stmt(Some(int(0))),
        ],
    )]);

    let executable = compile_program(&program);
    let function = executable
        .functions
        .get(FunctionId(0))
        .expect("compiled main function should exist");
    let body_state = function
        .states
        .get(StateId(2))
        .expect("while loop body state should exist");
    let instruction = body_state
        .instructions
        .get(InstructionId(0))
        .expect("continue should compile into a jump");

    assert!(matches!(
        instruction,
        waymark_vm_instructions_fullset::FullSet::CoreSet(
            waymark_vm_instructions_coreset::CoreSet::Jump { target_state }
        ) if *target_state == StateId(1)
    ));

    let result = completed_int(
        runtime_with_args(compile_program(&program), vec![TestValue::Int(0)])
            .run()
            .expect("program should complete"),
    );
    assert_eq!(result, 0);
}
