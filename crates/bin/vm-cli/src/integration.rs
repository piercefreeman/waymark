pub type InstructionSet = waymark_vm_instructions_fullset::FullSet<SampleSpec>;

pub type Executable = waymark_vm_bytecode::Executable<InstructionSet>;

#[derive(Debug)]
pub struct SampleSpec;

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
}

#[derive(Debug, Clone)]
pub struct SampleExtCallId(#[allow(dead_code)] pub usize);

impl waymark_vm_interpreter_coreset::value::ShouldJump for SampleValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        let SampleValue::Usize(val) = self;
        Ok(*val > 0)
    }
}

impl waymark_vm_interpreter_pureset::value::Add for SampleValue {
    fn add(a: &Self, b: &Self) -> Result<Self, waymark_vm_interpreter_pureset::value::AddError> {
        let SampleValue::Usize(a) = a;
        let SampleValue::Usize(b) = b;

        let val = a + b;

        Ok(SampleValue::Usize(val))
    }
}

impl From<SampleConstValue> for SampleValue {
    fn from(value: SampleConstValue) -> Self {
        let SampleConstValue::Usize(value) = value;
        SampleValue::Usize(value)
    }
}
