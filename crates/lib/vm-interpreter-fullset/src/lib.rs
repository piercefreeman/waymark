use waymark_vm_interpreter::ExecutionOutcome;

use waymark_vm_runtime_core::{Frame, Promise, RuntimeState};

pub struct FullSetInterpreter<Spec: waymark_vm_instructions_fullset::Spec, Executable> {
    pub core_set: waymark_vm_interpreter_coreset::CoreSetInterpreter<Spec, Executable, Spec::Value>,
    pub pure_set: waymark_vm_interpreter_pureset::PureSetInterpreter<Spec>,
}

impl<Spec: waymark_vm_instructions_fullset::Spec, Executable> Default
    for FullSetInterpreter<Spec, Executable>
{
    fn default() -> Self {
        Self {
            core_set: Default::default(),
            pure_set: Default::default(),
        }
    }
}

pub struct RuntimeView<'r, Executable, Value> {
    pub executable: &'r Executable,
    pub state: &'r mut RuntimeState<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error<Spec: waymark_vm_instructions_fullset::Spec>
where
    Spec::FunctionId: core::fmt::Debug,
{
    #[error(transparent)]
    CoreSet(#[from] waymark_vm_interpreter_coreset::Error<Spec>),

    #[error(transparent)]
    PureSet(#[from] waymark_vm_interpreter_pureset::Error),
}

pub enum Effect<Value, ExtCallId> {
    CoreSet(waymark_vm_interpreter_coreset::Effect<Value, ExtCallId>),
    PureSet(core::convert::Infallible),
}

impl<Spec, Executable> waymark_vm_interpreter::Interpreter for FullSetInterpreter<Spec, Executable>
where
    Spec: waymark_vm_instructions_fullset::Spec,
    Executable: 'static,
    Executable: waymark_vm_interpreter_coreset::FunctionInfo<Spec::FunctionId>,
    Spec: waymark_vm_instructions_coreset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
            FunctionId = waymark_vm_bytecode::FunctionId,
            StateId = waymark_vm_bytecode::StateId,
        >,
    Spec: waymark_vm_instructions_pureset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>
        + 'static,
    Spec::Value: Clone + 'static,
    Spec::Value: waymark_vm_interpreter_coreset::ShouldJump,
    Spec::Value: waymark_vm_interpreter_pureset::Value,
    Spec::ExtCallId: Clone,
{
    type RuntimeView<'r> = RuntimeView<'r, Executable, Spec::Value>;
    type Frame = Frame<Promise<Spec::Value>>;
    type Instruction = waymark_vm_instructions_fullset::FullSet<Spec>;
    type Error = Error<Spec>;
    type Effect = Effect<Spec::Value, Spec::ExtCallId>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Frame<Promise<Spec::Value>>,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Frame<Promise<Spec::Value>>, Self::Effect>, Self::Error> {
        let RuntimeView { executable, state } = runtime_view;
        Ok(match instruction {
            waymark_vm_instructions_fullset::FullSet::CoreSet(instruction) => {
                let runtime_view =
                    waymark_vm_interpreter_coreset::RuntimeView { executable, state };
                self.core_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::CoreSet)
            }
            waymark_vm_instructions_fullset::FullSet::PureSet(instruction) => {
                let runtime_view = ();
                self.pure_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::PureSet)
            }
        })
    }
}
