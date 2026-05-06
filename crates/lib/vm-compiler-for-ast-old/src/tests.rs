use waymark_vm_ast_old::{ActionCall, Call, Literal};
use waymark_vm_ast_old_helpers::{
    action_call, action_stmt, assignment, assignment_targets, break_stmt, conditional_stmt,
    continue_stmt, float, for_stmt, function, int, parallel_expr, program, return_stmt, variable,
};

use crate::{
    CompileError, compile,
    function::{compiler, table},
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestConstValue {
    Int(i64),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestExtCallId(String);

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestLiteralLoweringError {
    UnsupportedLiteral,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestActionLoweringError {}

struct TestLowering;

impl<Spec> crate::lowering::CoreSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_coreset::Spec<ExtCallId = TestExtCallId>,
{
    type ActionError = TestActionLoweringError;

    fn lower_action(call: &ActionCall) -> Result<Spec::ExtCallId, Self::ActionError> {
        Ok(TestExtCallId(call.action_name.clone()))
    }
}

impl<Spec> crate::lowering::PureSet<Spec> for TestLowering
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnitTestActionLoweringError {
    UnsupportedAction,
}

struct ActionFailingLowering;

impl<Spec> crate::lowering::CoreSet<Spec> for ActionFailingLowering
where
    Spec: waymark_vm_instructions_coreset::Spec<ExtCallId = TestExtCallId>,
{
    type ActionError = UnitTestActionLoweringError;

    fn lower_action(_call: &ActionCall) -> Result<Spec::ExtCallId, Self::ActionError> {
        Err(UnitTestActionLoweringError::UnsupportedAction)
    }
}

impl<Spec> crate::lowering::PureSet<Spec> for ActionFailingLowering
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

struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode_core::FunctionId;
    type StateId = waymark_vm_bytecode_core::StateId;
    type ExtCallId = TestExtCallId;
}

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type ConstValue = TestConstValue;
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
        CompileError::FunctionCompiler(compiler::Error::LiteralLowering {
            error: TestLiteralLoweringError::UnsupportedLiteral,
        })
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
fn rejects_copy_assignments_without_a_move_instruction() {
    let program = program(vec![function(
        "main",
        &[],
        vec![
            assignment("x", int(1)),
            assignment("y", variable("x")),
            return_stmt(Some(variable("y"))),
        ],
    )]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("copy assignment should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::AssignmentNeedsCopy { target }
        )) if target == "y"
    ));
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
        CompileError::FunctionCompiler(compiler::Error::LoopControlOutsideLoop { kind })
            if kind == "break"
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
        CompileError::FunctionCompiler(compiler::Error::LoopControlOutsideLoop { kind })
            if kind == "continue"
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
            compiler::Unsupported::Statement { kind }
        )) if kind == "ForLoop"
    ));
}

#[test]
fn rejects_single_target_parallel_expressions_that_need_aggregation() {
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

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("single-target parallel expressions should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::ParallelExprAssignment {
                target_count,
                call_count,
                ..
            }
        )) if target_count == 1 && call_count == 2
    ));
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
                ..
            }
        )) if target_count == 2 && call_count == 1
    ));
}
