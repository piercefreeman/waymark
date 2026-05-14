mod support;

use support::{TestValue, compile_program, runtime, runtime_with_args};
use waymark_vm_ast_old::{BinaryOperator, Call, Expr, Spanned, UnaryOperator};
use waymark_vm_ast_old_helpers::{
    action_call, action_expr, assignment, assignment_targets, binary_expr, break_stmt,
    conditional_stmt, continue_stmt, function, function_call, function_expr, int, parallel_expr,
    parallel_stmt, program, return_stmt, unary_expr, variable, while_stmt,
};
use waymark_vm_bytecode_core::{FunctionId, InstructionId, StateId};
use waymark_vm_compiler_for_ast_old_test_support::TestActionRef;
use waymark_vm_interpreter_fullset::Effect;

fn completed_int(effect: Effect<TestValue, TestActionRef>) -> i64 {
    match completed_value(effect) {
        TestValue::Int(value) => value,
        other => panic!("unexpected runtime effect: {other:?}"),
    }
}

fn completed_value(effect: Effect<TestValue, TestActionRef>) -> TestValue {
    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => value,
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
            return_stmt(Some(binary_expr(
                variable("x"),
                BinaryOperator::Add,
                variable("y"),
            ))),
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
fn compiles_scalar_binary_operations_to_completion() {
    struct BinaryCase {
        name: &'static str,
        inputs: Vec<&'static str>,
        expr: Spanned<Expr>,
        args: Vec<TestValue>,
        expected: TestValue,
    }

    let cases = vec![
        BinaryCase {
            name: "add",
            inputs: Vec::new(),
            expr: binary_expr(int(2), BinaryOperator::Add, int(3)),
            args: Vec::new(),
            expected: TestValue::Int(5),
        },
        BinaryCase {
            name: "sub",
            inputs: Vec::new(),
            expr: binary_expr(int(9), BinaryOperator::Sub, int(4)),
            args: Vec::new(),
            expected: TestValue::Int(5),
        },
        BinaryCase {
            name: "mul",
            inputs: Vec::new(),
            expr: binary_expr(int(6), BinaryOperator::Mul, int(7)),
            args: Vec::new(),
            expected: TestValue::Int(42),
        },
        BinaryCase {
            name: "div",
            inputs: Vec::new(),
            expr: binary_expr(int(8), BinaryOperator::Div, int(2)),
            args: Vec::new(),
            expected: TestValue::Int(4),
        },
        BinaryCase {
            name: "floor div",
            inputs: Vec::new(),
            expr: binary_expr(int(7), BinaryOperator::FloorDiv, int(2)),
            args: Vec::new(),
            expected: TestValue::Int(3),
        },
        BinaryCase {
            name: "mod",
            inputs: Vec::new(),
            expr: binary_expr(int(7), BinaryOperator::Mod, int(3)),
            args: Vec::new(),
            expected: TestValue::Int(1),
        },
        BinaryCase {
            name: "eq",
            inputs: Vec::new(),
            expr: binary_expr(int(4), BinaryOperator::Eq, int(4)),
            args: Vec::new(),
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "ne",
            inputs: Vec::new(),
            expr: binary_expr(int(4), BinaryOperator::Ne, int(5)),
            args: Vec::new(),
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "lt",
            inputs: Vec::new(),
            expr: binary_expr(int(1), BinaryOperator::Lt, int(2)),
            args: Vec::new(),
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "le",
            inputs: Vec::new(),
            expr: binary_expr(int(2), BinaryOperator::Le, int(2)),
            args: Vec::new(),
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "gt",
            inputs: Vec::new(),
            expr: binary_expr(int(3), BinaryOperator::Gt, int(2)),
            args: Vec::new(),
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "ge",
            inputs: Vec::new(),
            expr: binary_expr(int(3), BinaryOperator::Ge, int(3)),
            args: Vec::new(),
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "in",
            inputs: vec!["needle", "haystack"],
            expr: binary_expr(variable("needle"), BinaryOperator::In, variable("haystack")),
            args: vec![
                TestValue::Int(2),
                TestValue::List(vec![TestValue::Int(1), TestValue::Int(2)]),
            ],
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "not in",
            inputs: vec!["needle", "haystack"],
            expr: binary_expr(
                variable("needle"),
                BinaryOperator::NotIn,
                variable("haystack"),
            ),
            args: vec![
                TestValue::Int(4),
                TestValue::List(vec![TestValue::Int(1), TestValue::Int(2)]),
            ],
            expected: TestValue::Bool(true),
        },
        BinaryCase {
            name: "and falsey lhs",
            inputs: vec!["lhs", "rhs"],
            expr: binary_expr(variable("lhs"), BinaryOperator::And, variable("rhs")),
            args: vec![TestValue::Int(0), TestValue::Int(5)],
            expected: TestValue::Int(0),
        },
        BinaryCase {
            name: "and truthy lhs",
            inputs: vec!["lhs", "rhs"],
            expr: binary_expr(variable("lhs"), BinaryOperator::And, variable("rhs")),
            args: vec![TestValue::Int(2), TestValue::Int(5)],
            expected: TestValue::Int(5),
        },
        BinaryCase {
            name: "or falsey lhs",
            inputs: vec!["lhs", "rhs"],
            expr: binary_expr(variable("lhs"), BinaryOperator::Or, variable("rhs")),
            args: vec![TestValue::Int(0), TestValue::Int(5)],
            expected: TestValue::Int(5),
        },
        BinaryCase {
            name: "or truthy lhs",
            inputs: vec!["lhs", "rhs"],
            expr: binary_expr(variable("lhs"), BinaryOperator::Or, variable("rhs")),
            args: vec![TestValue::Int(2), TestValue::Int(5)],
            expected: TestValue::Int(2),
        },
    ];

    for case in cases {
        let program = program(vec![function(
            "main",
            case.inputs.as_slice(),
            vec![return_stmt(Some(case.expr.clone()))],
        )]);
        let executable = compile_program(&program);

        let effect = if case.args.is_empty() {
            runtime(executable)
                .run()
                .unwrap_or_else(|error| panic!("{} should complete: {error:?}", case.name))
        } else {
            runtime_with_args(executable, case.args.clone())
                .run()
                .unwrap_or_else(|error| panic!("{} should complete: {error:?}", case.name))
        };

        assert_eq!(completed_value(effect), case.expected, "{}", case.name);
    }
}

#[test]
fn compiles_scalar_unary_operations_to_completion() {
    struct UnaryCase {
        name: &'static str,
        inputs: Vec<&'static str>,
        expr: Spanned<Expr>,
        args: Vec<TestValue>,
        expected: TestValue,
    }

    let cases = vec![
        UnaryCase {
            name: "neg",
            inputs: Vec::new(),
            expr: unary_expr(UnaryOperator::Neg, int(5)),
            args: Vec::new(),
            expected: TestValue::Int(-5),
        },
        UnaryCase {
            name: "not falsey int",
            inputs: vec!["value"],
            expr: unary_expr(UnaryOperator::Not, variable("value")),
            args: vec![TestValue::Int(0)],
            expected: TestValue::Bool(true),
        },
        UnaryCase {
            name: "not truthy int",
            inputs: vec!["value"],
            expr: unary_expr(UnaryOperator::Not, variable("value")),
            args: vec![TestValue::Int(7)],
            expected: TestValue::Bool(false),
        },
        UnaryCase {
            name: "not empty list",
            inputs: vec!["value"],
            expr: unary_expr(UnaryOperator::Not, variable("value")),
            args: vec![TestValue::List(Vec::new())],
            expected: TestValue::Bool(true),
        },
    ];

    for case in cases {
        let program = program(vec![function(
            "main",
            case.inputs.as_slice(),
            vec![return_stmt(Some(case.expr.clone()))],
        )]);
        let executable = compile_program(&program);

        let effect = if case.args.is_empty() {
            runtime(executable)
                .run()
                .unwrap_or_else(|error| panic!("{} should complete: {error:?}", case.name))
        } else {
            runtime_with_args(executable, case.args.clone())
                .run()
                .unwrap_or_else(|error| panic!("{} should complete: {error:?}", case.name))
        };

        assert_eq!(completed_value(effect), case.expected, "{}", case.name);
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
            vec![return_stmt(Some(binary_expr(
                variable("value"),
                BinaryOperator::Add,
                int(1),
            )))],
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
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch"));
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
fn compiles_parallel_blocks_to_fan_out_before_awaiting() {
    let program = program(vec![
        function(
            "main",
            &[],
            vec![
                parallel_stmt(vec![
                    Call::Function(function_call("child", vec![int(3)])),
                    Call::Action(action_call("fetch", vec![("value", int(4))])),
                ]),
                return_stmt(Some(int(9))),
            ],
        ),
        function(
            "child",
            &["value"],
            vec![return_stmt(Some(variable("value")))],
        ),
    ]);

    let executable = compile_program(&program);
    let main = executable
        .functions
        .get(FunctionId(0))
        .expect("compiled main function should exist");
    let start_state = main
        .states
        .get(StateId(0))
        .expect("parallel block should start in state 0");
    let await_function_state = main
        .states
        .get(StateId(1))
        .expect("parallel block should await after starting all calls");
    let function_call = start_state
        .instructions
        .get(InstructionId(1))
        .expect("parallel block should issue the function call before awaiting");
    let extcall = start_state
        .instructions
        .get(InstructionId(3))
        .expect("parallel block should issue the extcall before awaiting");
    let await_instruction = await_function_state
        .instructions
        .get(InstructionId(0))
        .expect("parallel block should await after starting all calls");

    assert!(matches!(
        function_call,
        waymark_vm_instructions_fullset::FullSet::CoreSet(
            waymark_vm_instructions_coreset::CoreSet::Call { .. }
        )
    ));
    assert!(matches!(
        extcall,
        waymark_vm_instructions_fullset::FullSet::ExtCallSet(
            waymark_vm_instructions_extcallset::ExtCallSet::ActionCall { resume, .. }
        ) if *resume == StateId(1)
    ));
    assert!(matches!(
        await_instruction,
        waymark_vm_instructions_fullset::FullSet::CoreSet(
            waymark_vm_instructions_coreset::CoreSet::Await { .. }
        )
    ));
}

#[test]
fn compiles_parallel_action_blocks_with_multiple_outstanding_extcalls() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            parallel_stmt(vec![
                Call::Action(action_call("fetch_first", vec![("value", int(1))])),
                Call::Action(action_call("fetch_second", vec![("value", int(2))])),
            ]),
            return_stmt(Some(int(7))),
        ],
    )]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let first_promise = match runtime
        .run()
        .expect("first run should emit the first extcall")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_first"));
            assert_eq!(args, vec![TestValue::Int(1)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    let second_promise = match runtime
        .run()
        .expect("second run should emit the second extcall")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_second"));
            assert_eq!(args, vec![TestValue::Int(2)]);
            promise_state_id
        }
        other => panic!("unexpected second runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(second_promise, TestValue::Int(20))
        .expect("second extcall promise should resolve");
    runtime
        .resolve_promise(first_promise, TestValue::Int(10))
        .expect("first extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolving both extcalls");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 7),
        other => panic!("unexpected final runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_mixed_parallel_blocks_with_leading_action_before_awaiting() {
    let program = program(vec![
        function(
            "main",
            &[],
            vec![
                parallel_stmt(vec![
                    Call::Action(action_call("fetch_first", vec![("value", int(1))])),
                    Call::Function(function_call("child", vec![int(2)])),
                    Call::Action(action_call("fetch_second", vec![("value", int(3))])),
                ]),
                return_stmt(Some(int(7))),
            ],
        ),
        function(
            "child",
            &["value"],
            vec![return_stmt(Some(variable("value")))],
        ),
    ]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let first_promise = match runtime
        .run()
        .expect("first run should emit the first extcall")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_first"));
            assert_eq!(args, vec![TestValue::Int(1)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    let second_promise = match runtime
        .run()
        .expect("second run should emit the second extcall before awaiting the first")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_second"));
            assert_eq!(args, vec![TestValue::Int(3)]);
            promise_state_id
        }
        other => panic!("unexpected second runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(second_promise, TestValue::Int(30))
        .expect("second extcall promise should resolve");
    runtime
        .resolve_promise(first_promise, TestValue::Int(10))
        .expect("first extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolving both extcalls");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 7),
        other => panic!("unexpected final runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_empty_parallel_blocks_as_noops() {
    let program = program(vec![function(
        "main",
        &[],
        vec![parallel_stmt(Vec::new()), return_stmt(Some(int(5)))],
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
fn compiles_parallel_expressions_into_positional_assignments() {
    let program = program(vec![
        function(
            "main",
            &[],
            vec![
                assignment_targets(
                    &["left", "right"],
                    parallel_expr(vec![
                        Call::Function(function_call("child", vec![int(3)])),
                        Call::Action(action_call("fetch", vec![("value", int(4))])),
                    ]),
                ),
                return_stmt(Some(binary_expr(
                    variable("left"),
                    BinaryOperator::Add,
                    variable("right"),
                ))),
            ],
        ),
        function(
            "child",
            &["value"],
            vec![return_stmt(Some(binary_expr(
                variable("value"),
                BinaryOperator::Add,
                int(1),
            )))],
        ),
    ]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let promise_state_id = match runtime.run().expect("program should emit an extcall") {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch"));
            assert_eq!(args, vec![TestValue::Int(4)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(5))
        .expect("extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolving the parallel expression");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 9),
        other => panic!("unexpected second runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_mixed_parallel_expressions_with_leading_action_before_awaiting() {
    let program = program(vec![
        function(
            "main",
            &[],
            vec![
                assignment_targets(
                    &["first", "second", "third"],
                    parallel_expr(vec![
                        Call::Action(action_call("fetch_first", vec![("value", int(1))])),
                        Call::Function(function_call("child", vec![int(2)])),
                        Call::Action(action_call("fetch_second", vec![("value", int(3))])),
                    ]),
                ),
                return_stmt(Some(binary_expr(
                    variable("first"),
                    BinaryOperator::Add,
                    binary_expr(variable("second"), BinaryOperator::Add, variable("third")),
                ))),
            ],
        ),
        function(
            "child",
            &["value"],
            vec![return_stmt(Some(binary_expr(
                variable("value"),
                BinaryOperator::Add,
                int(1),
            )))],
        ),
    ]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let first_promise = match runtime
        .run()
        .expect("first run should emit the first extcall")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_first"));
            assert_eq!(args, vec![TestValue::Int(1)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    let second_promise = match runtime
        .run()
        .expect("second run should emit the second extcall before awaiting the first")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_second"));
            assert_eq!(args, vec![TestValue::Int(3)]);
            promise_state_id
        }
        other => panic!("unexpected second runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(second_promise, TestValue::Int(30))
        .expect("second extcall promise should resolve");
    runtime
        .resolve_promise(first_promise, TestValue::Int(10))
        .expect("first extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolving both extcalls");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 43),
        other => panic!("unexpected final runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_parallel_expressions_into_aggregate_lists() {
    let program = program(vec![
        function(
            "main",
            &[],
            vec![
                assignment(
                    "results",
                    parallel_expr(vec![
                        Call::Function(function_call("child", vec![int(3)])),
                        Call::Action(action_call("fetch", vec![("value", int(4))])),
                    ]),
                ),
                return_stmt(Some(variable("results"))),
            ],
        ),
        function(
            "child",
            &["value"],
            vec![return_stmt(Some(binary_expr(
                variable("value"),
                BinaryOperator::Add,
                int(1),
            )))],
        ),
    ]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let promise_state_id = match runtime.run().expect("program should emit an extcall") {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch"));
            assert_eq!(args, vec![TestValue::Int(4)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(promise_state_id, TestValue::Int(5))
        .expect("extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolving the parallel expression");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::List(
            values,
        ))) => assert_eq!(values, vec![TestValue::Int(4), TestValue::Int(5)]),
        other => panic!("unexpected second runtime effect: {other:?}"),
    }
}

#[test]
fn compiles_parallel_expression_results_by_call_position() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment_targets(
                &["first", "second"],
                parallel_expr(vec![
                    Call::Action(action_call("fetch_first", vec![("value", int(1))])),
                    Call::Action(action_call("fetch_second", vec![("value", int(2))])),
                ]),
            ),
            return_stmt(Some(binary_expr(
                variable("first"),
                BinaryOperator::Add,
                variable("second"),
            ))),
        ],
    )]);

    let executable = compile_program(&program);
    let mut runtime = runtime(executable);

    let first_promise = match runtime
        .run()
        .expect("first run should emit the first extcall")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_first"));
            assert_eq!(args, vec![TestValue::Int(1)]);
            promise_state_id
        }
        other => panic!("unexpected first runtime effect: {other:?}"),
    };

    let second_promise = match runtime
        .run()
        .expect("second run should emit the second extcall")
    {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert!(matches!(&action_ref, TestActionRef(name) if name == "fetch_second"));
            assert_eq!(args, vec![TestValue::Int(2)]);
            promise_state_id
        }
        other => panic!("unexpected second runtime effect: {other:?}"),
    };

    runtime
        .resolve_promise(second_promise, TestValue::Int(20))
        .expect("second extcall promise should resolve");
    runtime
        .resolve_promise(first_promise, TestValue::Int(10))
        .expect("first extcall promise should resolve");

    let effect = runtime
        .run()
        .expect("program should complete after resolving both parallel expression calls");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(TestValue::Int(
            value,
        ))) => assert_eq!(value, 30),
        other => panic!("unexpected final runtime effect: {other:?}"),
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

#[test]
fn compiles_nested_conditionals_inside_while_loops() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            assignment("count", int(0)),
            while_stmt(
                variable("flag"),
                vec![conditional_stmt(
                    variable("count"),
                    vec![break_stmt()],
                    Vec::new(),
                    Some(vec![assignment(
                        "count",
                        binary_expr(variable("count"), BinaryOperator::Add, int(1)),
                    )]),
                )],
            ),
            return_stmt(Some(variable("count"))),
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
    assert_eq!(result, 0);
}
