//! Test support types for `vm-compiler-for-ast-old` crates.
//!
//! This crate provides a small VM spec plus lowering implementation used by
//! compiler tests to assert on emitted bytecode without depending on a real
//! production lowering.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use waymark_vm_ast_old::{ActionCall, Literal};

/// Constant values used by the test VM spec.
#[derive(Debug, Clone)]
pub enum TestConstValue {
    /// Integer constant.
    Int(i64),

    /// `None` constant.
    None,
}

/// Extcall identifier used by the test VM spec.
#[derive(Debug, Clone)]
pub struct TestExtCallId(
    /// Action name captured from the AST call.
    pub String,
);

/// Errors produced while lowering literals in tests.
#[derive(Debug, Clone)]
pub enum TestLiteralError {
    /// The literal variant is intentionally unsupported by the test lowering.
    UnsupportedLiteral,
}

/// Errors produced while lowering actions in tests.
#[derive(Debug, Clone)]
pub enum TestActionError {
    /// Placeholder unsupported-action error.
    Unsupported,
}

/// Minimal VM spec used by compiler tests.
#[derive(Debug)]
pub struct TestSpec;

impl waymark_vm_instructions_coreset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode_core::FunctionId;
    type StateId = waymark_vm_bytecode_core::StateId;
}

impl waymark_vm_instructions_extcallset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type StateId = waymark_vm_bytecode_core::StateId;
    type ExtCallId = TestExtCallId;
}

impl waymark_vm_instructions_pureset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type ConstValue = TestConstValue;
}

/// Convenience alias for executables compiled against [`TestSpec`].
pub type TestExecutable = waymark_vm_compiler_for_ast_old_core::ExecutableFor<TestSpec>;

/// Lowering implementation used by compiler tests.
pub struct TestLowering;

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::ExtCallSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_extcallset::Spec<ExtCallId = TestExtCallId>,
{
    type ActionError = TestActionError;

    fn lower_action(call: &ActionCall) -> Result<Spec::ExtCallId, Self::ActionError> {
        Ok(TestExtCallId(call.action_name.clone()))
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::PureSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = TestConstValue>,
{
    type LiteralError = TestLiteralError;

    fn lower_literal(literal: &Literal) -> Result<Spec::ConstValue, Self::LiteralError> {
        match literal {
            Literal::Int(value) => Ok(TestConstValue::Int(*value)),
            Literal::None => Ok(TestConstValue::None),
            Literal::Float(_) | Literal::String(_) | Literal::Bool(_) => {
                Err(TestLiteralError::UnsupportedLiteral)
            }
        }
    }
}
