//! Test support types for `vm-compiler-for-ast-old` crates.
//!
//! This crate provides a small VM spec used by compiler tests and wraps the
//! shared bytecode-const lowering in test-named types.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use waymark_vm_ast_old::{ActionCall, Literal};

/// Test value type definition as an actual [`waymark_vm_value_python::Value`].
pub use waymark_vm_value_python::Value as TestValue;

/// Test ready value type definition as an actual [`waymark_vm_value_python::ReadyValue`].
pub use waymark_vm_value_python::ReadyValue as TestReadyValue;

/// Test raised exception type definition as an actual
/// [`waymark_vm_value_python::RaisedException`].
pub use waymark_vm_value_python::RaisedException as TestRaisedException;

/// Test const value type definition as
/// an actual [`waymark_vm_compiler_for_ast_old_bytecode_consts::ConstValue`].
pub use waymark_vm_compiler_for_ast_old_bytecode_consts::ConstValue as TestConstValue;

/// Test const exception type definition as
/// an actual [`waymark_vm_compiler_for_ast_old_bytecode_consts::ConstException`].
pub use waymark_vm_compiler_for_ast_old_bytecode_consts::ConstException as TestConstException;
pub use waymark_vm_compiler_for_ast_old_bytecode_consts::ConstExceptionPattern as TestConstExceptionPattern;

/// Errors produced while lowering literals in tests.
pub use waymark_vm_compiler_for_ast_old_bytecode_consts::LoweringError as TestLiteralLoweringError;

/// Action reference used by the test VM spec.
#[derive(Debug, Clone)]
pub struct TestActionRef(
    /// Action name captured from the AST call.
    pub String,
);

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

impl waymark_vm_instructions_excset::Spec for TestSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type StateId = waymark_vm_bytecode_core::StateId;
    type ConstException = TestConstException;
    type ConstExceptionPattern = TestConstExceptionPattern;
}

/// Convenience alias for executables compiled against [`TestSpec`].
pub type TestExecutable = waymark_vm_compiler_for_ast_old_core::ExecutableFor<TestSpec>;

/// Lowering implementation used by compiler tests.
pub struct TestLowering;

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::ExtCallSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = TestActionRef>,
{
    type ActionError = core::convert::Infallible;

    fn lower_action(call: &ActionCall) -> Result<Spec::ActionRef, Self::ActionError> {
        Ok(TestActionRef(call.action_name.clone()))
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::PureSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = TestConstValue>,
{
    type LiteralError = TestLiteralLoweringError;

    fn lower_literal(literal: &Literal) -> Result<Spec::ConstValue, Self::LiteralError> {
        TestConstValue::lower(literal)
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::ExcSet<Spec> for TestLowering
where
    Spec: waymark_vm_instructions_excset::Spec<
            ConstException = TestConstException,
            ConstExceptionPattern = TestConstExceptionPattern,
        >,
{
    fn lower_exception_pattern(class_names: &[String]) -> Spec::ConstExceptionPattern {
        waymark_vm_compiler_for_ast_old_bytecode_consts::lower_exception_pattern(class_names)
    }

    fn lower_any_exception_pattern() -> Spec::ConstExceptionPattern {
        waymark_vm_compiler_for_ast_old_bytecode_consts::lower_any_exception_pattern()
    }

    fn lower_compiler_emitted_exception(
        exception: &waymark_vm_compiler_for_ast_old_core::lowering::CompilerEmittedException,
    ) -> Spec::ConstException {
        TestConstException::lower(exception)
    }
}
