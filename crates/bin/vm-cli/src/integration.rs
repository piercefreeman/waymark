pub type InstructionSet = waymark_vm_instructions_fullset::FullSet<SampleSpec>;

pub type Executable = waymark_vm_bytecode::Executable<InstructionSet>;

#[derive(Debug)]
pub struct SampleSpec;

pub struct SampleLowering;

impl waymark_vm_instructions_coreset::Spec for SampleSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type FunctionId = waymark_vm_bytecode_core::FunctionId;
    type ExtCallId = SampleExtCallId;
    type StateId = waymark_vm_bytecode_core::StateId;
}

impl waymark_vm_instructions_pureset::Spec for SampleSpec {
    type RegisterId = waymark_vm_runtime_core::RegisterId;
    type ConstValue = SampleConstValue;
}

#[derive(Debug, Clone)]
pub enum SampleConstValue {
    Usize(usize),
}

#[derive(Debug, Clone)]
pub enum SampleValue {
    Usize(usize),
    List(#[allow(dead_code)] Vec<SampleValue>),
}

#[derive(Debug, Clone)]
pub struct SampleExtCallId(#[allow(dead_code)] pub usize);

impl waymark_vm_interpreter_coreset::value::ShouldJump for SampleValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        match self {
            SampleValue::Usize(val) => Ok(*val > 0),
            SampleValue::List(_) => {
                Err(waymark_vm_interpreter_coreset::value::NotAConditionalError)
            }
        }
    }
}

impl waymark_vm_interpreter_pureset::value::Add for SampleValue {
    fn add(a: &Self, b: &Self) -> Result<Self, waymark_vm_interpreter_pureset::value::AddError> {
        match (a, b) {
            (SampleValue::Usize(a), SampleValue::Usize(b)) => Ok(SampleValue::Usize(a + b)),
            _ => Err(waymark_vm_interpreter_pureset::value::AddError::NotAddable),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::MakeList for SampleValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self>,
    {
        Ok(SampleValue::List(items.into_iter().collect()))
    }
}

impl From<SampleConstValue> for SampleValue {
    fn from(value: SampleConstValue) -> Self {
        let SampleConstValue::Usize(value) = value;
        SampleValue::Usize(value)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported lowering")]
pub struct UnsupportedLoweringError;

impl<Spec> waymark_vm_compiler_for_ast_old::lowering::CoreSet<Spec> for SampleLowering
where
    Spec: waymark_vm_instructions_coreset::Spec<ExtCallId = SampleExtCallId>,
{
    type ActionError = UnsupportedLoweringError;

    fn lower_action(
        _call: &waymark_vm_ast_old::ActionCall,
    ) -> Result<Spec::ExtCallId, Self::ActionError> {
        Ok(SampleExtCallId(1337))
    }
}

impl<Spec> waymark_vm_compiler_for_ast_old::lowering::PureSet<Spec> for SampleLowering
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
