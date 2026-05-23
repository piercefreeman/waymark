//! Unit tests for the AST-to-bytecode compiler, grouped by feature area.

use waymark_vm_ast_old::{ActionCall, Literal};
use waymark_vm_compiler_for_ast_old_core::lowering;
use waymark_vm_compiler_for_ast_old_test_support::{
    TestActionRef, TestConstValue, TestLiteralLoweringError, TestLowering,
};

mod conditionals;
mod for_loops;
mod function_table;
mod loops;
mod lowering_errors;
mod parallel;
mod variables;

/// Distinguishable error variant raised by [`ActionFailingLowering`] so tests
/// can pattern-match on the propagated action-lowering failure.
#[derive(Debug, Clone, PartialEq, Eq)]
enum UnitTestActionLoweringError {
    UnsupportedAction,
}

/// Lowering implementation that always rejects action calls, used to verify
/// that downstream action-lowering errors flow through the compiler unchanged.
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
