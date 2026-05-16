use waymark_vm_ast_old::{ActionCall, BinaryOperator, Call, Expr, GlobalFunction, Kwarg, Literal};
use waymark_vm_ast_old_helpers::{
    action_call, action_stmt, assignment, assignment_targets, binary_expr, break_stmt,
    builtin_function_call, conditional_stmt, continue_stmt, enumerate_expr, float, for_stmt,
    function, int, parallel_expr, program, range_expr, return_stmt, spanned, variable, while_stmt,
};
use waymark_vm_compiler_for_ast_old_core::lowering;
use waymark_vm_compiler_for_ast_old_test_support::{
    TestActionRef, TestConstValue, TestLiteralLoweringError, TestLowering, TestSpec,
};
use waymark_vm_instructions_fullset::FullSet;
use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_runtime_core::RegisterId;

use crate::{
    CompileError, compile,
    function::{compiler, table},
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnitTestActionLoweringError {
    UnsupportedAction,
}

struct ActionFailingLowering;

impl<Spec> lowering::ExtCallSet<Spec> for ActionFailingLowering
where
    Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = TestActionRef>,
{
    type ActionError = UnitTestActionLoweringError;

    fn lower_action(_call: &ActionCall) -> Result<Spec::ActionRef, Self::ActionError> {
        Err(UnitTestActionLoweringError::UnsupportedAction)
    }
}

impl<Spec> lowering::PureSet<Spec> for ActionFailingLowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = TestConstValue>,
{
    type LiteralError = TestLiteralLoweringError;

    fn lower_literal(literal: &Literal) -> Result<Spec::ConstValue, Self::LiteralError> {
        <TestLowering as lowering::PureSet<Spec>>::lower_literal(literal)
    }
}

#[test]
fn rejects_duplicate_function_names() {
    let program = program(vec![
        function("main", &[], vec![return_stmt(Some(int(1)))]),
        function("main", &[], vec![return_stmt(Some(int(2)))]),
    ]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("duplicate function names should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionTable(table::Error::DuplicateFunction { name }) if name == "main"
    ));
}

#[test]
fn rejects_duplicate_inputs() {
    let program = program(vec![function("main", &["value", "value"], vec![])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("duplicate inputs should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::DuplicateInput { function, name })
            if function == "main" && name == "value"
    ));
}

#[test]
fn rejects_unknown_variables() {
    let program = program(vec![function(
        "main",
        &[],
        vec![return_stmt(Some(variable("missing")))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("unknown variables should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "missing"
    ));
}

#[test]
fn rejects_self_referential_assignments_to_new_variables() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment("value", variable("value"))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("self-referential assignment should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "value"
    ));
}

#[test]
fn rejects_non_finite_float_literals() {
    let program = program(vec![function(
        "main",
        &[],
        vec![return_stmt(Some(float(f64::NAN)))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("non-finite literals should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LiteralLowering(
            TestLiteralLoweringError::InvalidFloat(_)
        ))
    ));
}

#[test]
fn preserves_action_lowering_errors() {
    let program = program(vec![function("main", &[], vec![action_stmt("notify")])]);

    let error = match compile::<TestSpec, ActionFailingLowering>(&program) {
        Ok(_) => panic!("unsupported actions should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::ActionLowering {
            action_name,
            error: UnitTestActionLoweringError::UnsupportedAction,
        }) if action_name == "notify"
    ));
}

#[test]
fn lowers_copy_assignments_with_a_copy_instruction() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("x", int(1)),
            assignment("y", variable("x")),
            return_stmt(Some(variable("y"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("copy assignment should lower successfully");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");
    let state = function
        .states
        .iter()
        .next()
        .expect("entry state should exist");

    assert!(state.instructions.iter().any(|instruction| {
        matches!(
            instruction,
            FullSet::PureSet(PureSet::Copy {
                dst: RegisterId(1),
                src: RegisterId(0),
            })
        )
    }));
}

#[test]
fn rejects_break_outside_loops() {
    let program = program(vec![function("main", &[], vec![break_stmt()])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("break outside a loop should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LoopControlOutsideLoop {
            kind: compiler::LoopControlKind::Break
        })
    ));
}

#[test]
fn rejects_continue_outside_loops() {
    let program = program(vec![function("main", &[], vec![continue_stmt()])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("continue outside a loop should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LoopControlOutsideLoop {
            kind: compiler::LoopControlKind::Continue
        })
    ));
}

#[test]
fn rejects_variables_missing_on_some_conditional_paths() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                Vec::new(),
                None,
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("partially initialized conditionals should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "x"
    ));
}

#[test]
fn allows_variables_initialized_on_all_conditional_paths() {
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

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("variables initialized on every path should compile");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");

    assert_eq!(function.num_regs, 2);
}

#[test]
fn allows_variables_initialized_on_all_if_elif_and_else_paths() {
    let program = program(vec![function(
        "main",
        &["flag", "fallback"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                vec![(variable("fallback"), vec![assignment("x", int(2))])],
                Some(vec![assignment("x", int(3))]),
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("variables initialized on every if/elif/else path should compile");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");

    assert_eq!(function.num_regs, 3);
}

#[test]
fn rejects_variables_missing_on_an_elif_path() {
    let program = program(vec![function(
        "main",
        &["flag", "fallback"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                vec![(variable("fallback"), Vec::new())],
                Some(vec![assignment("x", int(3))]),
            ),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("missing elif initialization should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "x"
    ));
}

#[test]
fn reuses_function_scope_locals_after_branch_only_declarations() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            conditional_stmt(
                variable("flag"),
                vec![assignment("x", int(1))],
                Vec::new(),
                None,
            ),
            assignment("x", int(2)),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let executable = compile::<TestSpec, TestLowering>(&program)
        .expect("locals declared in a branch should stay bound at function scope");
    let function = executable
        .functions
        .iter()
        .next()
        .expect("compiled main function should exist");

    assert_eq!(function.num_regs, 2);
}

#[test]
fn rejects_variables_initialized_only_inside_while_loops() {
    let program = program(vec![function(
        "main",
        &["flag"],
        vec![
            while_stmt(variable("flag"), vec![assignment("x", int(1))]),
            return_stmt(Some(variable("x"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("loop-only initialization should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "x"
    ));
}

#[test]
fn compiles_for_loops_over_lists() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["item"],
                variable("items"),
                vec![assignment(
                    "total",
                    binary_expr(variable("total"), BinaryOperator::Add, variable("item")),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("list for loops should compile");
}

#[test]
fn compiles_for_loops_over_range() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["item"],
                range_expr(vec![int(1), int(4)]),
                vec![assignment(
                    "total",
                    binary_expr(variable("total"), BinaryOperator::Add, variable("item")),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("range for loops should compile");
}

#[test]
fn compiles_for_loops_over_enumerate() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                enumerate_expr(variable("items")),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("enumerate for loops should compile");
}

#[test]
fn compiles_enumerate_over_range_for_loops() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                enumerate_expr(range_expr(vec![int(2), int(5)])),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program).expect("enumerated range for loops should compile");
}

#[test]
fn compiles_for_loops_over_enumerate_items_kwarg() {
    let mut call = builtin_function_call(GlobalFunction::Enumerate, Vec::new());
    call.kwargs.push(Kwarg {
        name: "items".to_owned(),
        value: variable("items"),
    });

    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            assignment("total", int(0)),
            for_stmt(
                &["idx", "item"],
                spanned(Expr::FunctionCall { call }),
                vec![assignment(
                    "total",
                    binary_expr(
                        variable("total"),
                        BinaryOperator::Add,
                        binary_expr(variable("idx"), BinaryOperator::Add, variable("item")),
                    ),
                )],
            ),
            return_stmt(Some(variable("total"))),
        ],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("enumerate(items=...) for loops should compile");
}

#[test]
fn rejects_for_loops_over_range_with_too_many_args() {
    let program = program(vec![function(
        "main",
        &[],
        vec![for_stmt(
            &["item"],
            range_expr(vec![int(0), int(4), int(1), int(2)]),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("range with too many args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::FunctionArityMismatch {
            function,
            expected,
            actual,
        }) if function == "range" && expected == 3 && actual == 4
    ));
}

#[test]
fn rejects_for_loops_over_enumerate_with_too_many_args() {
    let program = program(vec![function(
        "main",
        &["items", "other"],
        vec![for_stmt(
            &["idx", "item"],
            spanned(Expr::FunctionCall {
                call: builtin_function_call(
                    GlobalFunction::Enumerate,
                    vec![variable("items"), variable("other")],
                ),
            }),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("enumerate with too many args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::FunctionArityMismatch {
            function,
            expected,
            actual,
        }) if function == "enumerate" && expected == 1 && actual == 2
    ));
}

#[test]
fn rejects_for_loops_over_enumerate_with_mixed_positional_and_keyword_args() {
    let mut call = builtin_function_call(GlobalFunction::Enumerate, vec![variable("items")]);
    call.kwargs.push(Kwarg {
        name: "items".to_owned(),
        value: variable("other"),
    });

    let program = program(vec![function(
        "main",
        &["items", "other"],
        vec![for_stmt(
            &["idx", "item"],
            spanned(Expr::FunctionCall { call }),
            Vec::new(),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("enumerate with mixed args should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::FunctionCall { name, reason }
        )) if name == "enumerate"
            && reason == compiler::UnsupportedFunctionCall::KeywordArguments
    ));
}

#[test]
fn rejects_loop_variables_initialized_only_inside_for_loops() {
    let program = program(vec![function(
        "main",
        &["items"],
        vec![
            for_stmt(&["item"], variable("items"), vec![continue_stmt()]),
            return_stmt(Some(variable("item"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("loop-only bindings should fail outside the for loop"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::UnknownVariable { name })
            if name == "item"
    ));
}

#[test]
fn allows_single_target_parallel_expressions_for_aggregation() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment(
            "results",
            parallel_expr(vec![
                Call::Action(action_call("first", Vec::new())),
                Call::Action(action_call("second", Vec::new())),
            ]),
        )],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("single-target parallel expressions should compile");
}

#[test]
fn allows_empty_single_target_parallel_expressions_for_aggregation() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment("results", parallel_expr(Vec::new()))],
    )]);

    compile::<TestSpec, TestLowering>(&program)
        .expect("empty single-target parallel expressions should compile");
}

#[test]
fn rejects_parallel_expressions_with_mismatched_targets() {
    let program = program(vec![function(
        "main",
        &[],
        vec![assignment_targets(
            &["left", "right"],
            parallel_expr(vec![Call::Action(action_call("only", Vec::new()))]),
        )],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("mismatched parallel expressions should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::ParallelExprAssignment {
                target_count,
                call_count,
                reason,
                ..
            }
        )) if target_count.get() == 2
            && call_count == 1
            && reason == compiler::UnsupportedParallelExprAssignment::TargetCountMustMatchCalls
    ));
}
