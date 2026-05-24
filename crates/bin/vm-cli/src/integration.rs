pub type SampleValue = waymark_vm_value::Value;
pub type SampleReadyValue = waymark_vm_value::ReadyValue;

#[cfg(test)]
static_assertions::assert_impl_all!(SampleValue: waymark_vm_interpreter_fullset::Value);

pub type InstructionSet = waymark_vm_instructions_fullset::FullSet<SampleSpec>;

pub type Executable = waymark_vm_bytecode::Executable<InstructionSet>;

#[derive(Debug)]
pub struct SampleSpec;

pub struct SampleLowering;

impl waymark_vm_instructions_coreset::Spec for SampleSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode_core::FunctionId;
    type StateId = waymark_vm_bytecode_core::StateId;
}

impl waymark_vm_instructions_extcallset::Spec for SampleSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type StateId = waymark_vm_bytecode_core::StateId;
    type ActionRef = SampleActionRef;
}

impl waymark_vm_instructions_pureset::Spec for SampleSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type ConstValue = SampleConstValue;
}

#[derive(Debug, Clone)]
pub enum SampleConstValue {
    Usize(usize),
}

impl From<&SampleConstValue> for SampleReadyValue {
    fn from(value: &SampleConstValue) -> Self {
        let SampleConstValue::Usize(value) = value;
        let value: i64 = (*value)
            .try_into()
            .expect("sample const values should fit in i64");
        SampleReadyValue::Int(value)
    }
}

#[derive(Debug, Clone)]
pub struct SampleActionRef(#[allow(dead_code)] pub usize);

#[derive(Debug, thiserror::Error)]
#[error("unsupported lowering")]
pub struct UnsupportedLoweringError;

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::ExtCallSet<Spec> for SampleLowering
where
    Spec: waymark_vm_instructions_extcallset::Spec<ActionRef = SampleActionRef>,
{
    type ActionError = UnsupportedLoweringError;

    fn lower_action(
        _call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<Spec::ActionRef, Self::ActionError> {
        Ok(SampleActionRef(1337))
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old_core::lowering::PureSet<Spec> for SampleLowering
where
    Spec: waymark_vm_instructions_pureset::Spec<ConstValue = SampleConstValue>,
{
    type LiteralError = UnsupportedLoweringError;

    fn lower_literal(
        literal: &waymark_vm_ast_old::Literal,
    ) -> Result<Spec::ConstValue, Self::LiteralError> {
        Ok(match literal {
            waymark_vm_ast_old::Literal::Int(val) => {
                let val: u64 = (*val).try_into().map_err(|_| UnsupportedLoweringError)?;
                let val: usize = val.try_into().map_err(|_| UnsupportedLoweringError)?;
                SampleConstValue::Usize(val)
            }
            waymark_vm_ast_old::Literal::Float(_)
            | waymark_vm_ast_old::Literal::String(_)
            | waymark_vm_ast_old::Literal::Bool(_)
            | waymark_vm_ast_old::Literal::None => return Err(UnsupportedLoweringError),
        })
    }
}
