//! A crate that assembles various waymark VM types and traits into
//! a single and coherent system of interconntected types.

use std::sync::Arc;

pub use waymark_vm_compiler_for_ast_old_const_value::ConstValue;
pub use waymark_vm_value_python::{ReadyValue, Value};

#[cfg(test)]
static_assertions::assert_impl_all!(Value: waymark_vm_interpreter_fullset::Value);

pub type InstructionSet = waymark_vm_instructions_fullset::FullSet<Spec>;

pub type Executable = waymark_vm_bytecode::Executable<InstructionSet>;

pub type Interpreter =
    waymark_vm_interpreter_fullset::FullSetInterpreter<Spec, Arc<Executable>, Value>;

pub type Runtime = waymark_vm_runtime::Runtime<Arc<Executable>, Interpreter, Value>;

pub type CallSpec = waymark_vm_runtime::CallSpec<waymark_vm_bytecode_core::FunctionId, Value>;

#[derive(Debug)]
pub struct Spec;

pub struct Lowering;

impl waymark_vm_instructions_coreset::Spec for Spec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode_core::FunctionId;
    type StateId = waymark_vm_bytecode_core::StateId;
}

impl waymark_vm_instructions_extcallset::Spec for Spec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type StateId = waymark_vm_bytecode_core::StateId;
    type ActionRef = waymark_action_core::ActionRef;
}

impl waymark_vm_instructions_pureset::Spec for Spec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type ConstValue = ConstValue;
}

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::ExtCallSet<Spec> for Lowering
where
    Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = waymark_action_core::ActionRef>,
{
    type ActionError = core::convert::Infallible;

    fn lower_action(
        call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<Spec::ActionRef, Self::ActionError> {
        Ok(waymark_vm_compiler_for_ast_old_action_ref::lower_action_ref(call))
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::PureSet<Spec> for Lowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = ConstValue>,
{
    type LiteralError = waymark_vm_compiler_for_ast_old_const_value::LoweringError;

    fn lower_literal(
        literal: &waymark_vm_ast_old::Literal,
    ) -> Result<Spec::ConstValue, Self::LiteralError> {
        ConstValue::lower(literal)
    }
}
