//! Test support types for `vm-compiler-for-ast-old` crates.
//!
//! This crate provides a small VM spec plus lowering implementation used by
//! compiler tests to assert on emitted bytecode without depending on a real
//! production lowering.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use waymark_vm_ast_old::{ActionCall, Literal};

/// Runtime VM values used by compiler tests.
pub use waymark_vm_value::Value as TestValue;

/// Constant values used by the test VM spec.
pub type TestConstValue = TestValue;

/// Action reference used by the test VM spec.
#[derive(Debug, Clone)]
pub struct TestActionRef(
    /// Action name captured from the AST call.
    pub String,
);

/// Errors produced while lowering literals in tests.
#[derive(Debug, Clone)]
pub enum TestLiteralError {
    /// A floating-point literal could not be represented as a VM float.
    InvalidFloat,
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
    type ActionRef = TestActionRef;
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
    Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = TestActionRef>,
{
    type ActionError = TestActionError;

    fn lower_action(call: &ActionCall) -> Result<Spec::ActionRef, Self::ActionError> {
        Ok(TestActionRef(call.action_name.clone()))
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
            Literal::Float(value) => {
                let value = (*value)
                    .try_into()
                    .map_err(|_| TestLiteralError::InvalidFloat)?;
                Ok(TestConstValue::Float(value))
            }
            Literal::String(value) => Ok(TestConstValue::String(value.clone())),
            Literal::Bool(value) => Ok(TestConstValue::Bool(*value)),
            Literal::None => Ok(TestConstValue::None),
        }
    }
}
