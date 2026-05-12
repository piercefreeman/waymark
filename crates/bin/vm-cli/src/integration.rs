use waymark_nonzero_duration::NonZeroDuration;

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

#[derive(Debug, Clone)]
pub enum SampleValue {
    Usize(usize),
    List(#[allow(dead_code)] Vec<SampleValue>),
}

#[derive(Debug, Clone)]
pub struct SampleActionRef(#[allow(dead_code)] pub usize);

#[derive(Debug, thiserror::Error)]
pub enum SampleSleepDurationError {
    #[error("the value cannot be used as a sleep duration")]
    UnsupportedValue,

    #[error("sleep duration must be non-zero")]
    Zero,

    #[error("sleep duration is out of range")]
    OutOfRange,
}

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

impl waymark_vm_interpreter_pureset::value::BinaryOps for SampleValue {
    fn add(
        a: &Self,
        b: &Self,
    ) -> Result<Self, waymark_vm_interpreter_pureset::value::BinaryOperationError> {
        match (a, b) {
            (SampleValue::Usize(a), SampleValue::Usize(b)) => Ok(SampleValue::Usize(*a + *b)),
            _ => Err(
                waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                    operation: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                },
            ),
        }
    }
}

impl waymark_vm_interpreter_pureset::value::UnaryOps for SampleValue {}

impl waymark_vm_interpreter_pureset::value::MakeList for SampleValue {
    fn make_list<I>(items: I) -> Result<Self, waymark_vm_interpreter_pureset::value::MakeListError>
    where
        I: IntoIterator<Item = Self>,
    {
        Ok(SampleValue::List(items.into_iter().collect()))
    }
}

impl waymark_vm_interpreter_extcallset::value::SleepDuration for SampleValue {
    type Error = SampleSleepDurationError;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        match self {
            SampleValue::Usize(seconds) => {
                let seconds: u64 = (*seconds).try_into().map_err(|_| Self::Error::OutOfRange)?;
                NonZeroDuration::from_secs(seconds).ok_or(Self::Error::Zero)
            }
            SampleValue::List(_) => Err(Self::Error::UnsupportedValue),
        }
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
