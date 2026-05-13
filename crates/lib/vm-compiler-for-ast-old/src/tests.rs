use waymark_vm_ast_old::{ActionCall, Call, Literal};
use waymark_vm_ast_old_helpers::{
    action_call, action_stmt, assignment, assignment_targets, break_stmt, conditional_stmt,
    continue_stmt, float, for_stmt, function, int, parallel_expr, program, return_stmt, variable,
    while_stmt,
};
use waymark_vm_compiler_for_ast_old_core::lowering;
use waymark_vm_compiler_for_ast_old_test_support::{
    TestConstValue, TestExtCallId, TestLiteralError as TestLiteralLoweringError, TestLowering,
    TestSpec,
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

impl<Spec> lowering::CoreSet<Spec> for ActionFailingLowering
where
    Spec: waymark_vm_instructions_coreset::Spec<ExtCallId = TestExtCallId>,
{
    type ActionError = UnitTestActionLoweringError;

    fn lower_action(_call: &ActionCall) -> Result<Spec::ExtCallId, Self::ActionError> {
        Err(UnitTestActionLoweringError::UnsupportedAction)
    }
}

impl<Spec> lowering::PureSet<Spec> for ActionFailingLowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = TestConstValue>,
{
    type LiteralError = TestLiteralLoweringError;

    fn lower_literal(literal: &Literal) -> Result<Spec::ConstValue, Self::LiteralError> {
        match literal {
            Literal::Int(value) => Ok(TestConstValue::Int(*value)),
            Literal::None => Ok(TestConstValue::None),
            Literal::Float(_) | Literal::String(_) | Literal::Bool(_) => {
                Err(TestLiteralLoweringError::UnsupportedLiteral)
            }
        }
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
fn preserves_literal_lowering_errors() {
    let program = program(vec![function(
        "main",
        &[],
        vec![return_stmt(Some(float(1.5)))],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("unsupported literals should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::LiteralLowering(
            TestLiteralLoweringError::UnsupportedLiteral
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
fn rejects_unsupported_for_loops() {
    let program = program(vec![function(
        "main",
        &[],
        vec![for_stmt(&["item"], int(1), vec![])],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("for loops should stay unsupported"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::Statement {
                kind: compiler::UnsupportedStatementKind::ForLoop
            }
        ))
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
