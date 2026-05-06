use waymark_vm_ast_old::{ActionCall, Literal};
use waymark_vm_ast_old_helpers::{
    action_stmt, assignment, float, function, int, program, return_stmt, variable, while_stmt,
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
fn rejects_unsupported_control_flow() {
    let program = program(vec![function("main", &[], vec![while_stmt()])]);

    let error = match compile::<TestSpec, TestLowering>(&program) {
        Ok(_) => panic!("while loops should stay unsupported"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CompileError::FunctionCompiler(compiler::Error::Unsupported(
            compiler::Unsupported::Statement { kind }
        )) if kind == "WhileLoop"
    ));
}
