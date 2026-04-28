//! The interpreter for the "full" instructions set.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{CaptureRuntimeView as _, Frame, Promise};

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "full" instructions set.
#[derive_where(Default)]
pub struct FullSetInterpreter<Spec: waymark_vm_instructions_fullset::Spec, Executable, Value> {
    /// The coreset interpreter used for core instructions.
    pub core_set: waymark_vm_interpreter_coreset::CoreSetInterpreter<Spec, Executable, Value>,

    /// The pureset interpreter used for pure instructions.
    pub pure_set: waymark_vm_interpreter_pureset::PureSetInterpreter<
        Spec,
        Spec::FunctionId,
        Spec::StateId,
        Value,
    >,
}

/// The runtime view for the [`FullSetInterpreter`].
pub use waymark_vm_runtime_core::FullRuntimeView as RuntimeView;

/// The effect for the [`FullSetInterpreter`].
#[derive(Debug)]
pub enum Effect<Value, ExtCallId> {
    /// An effect produced while executing a coreset instruction.
    CoreSet(waymark_vm_interpreter_coreset::Effect<Value, ExtCallId>),

    /// An impossible effect produced while executing a pureset instruction.
    PureSet(core::convert::Infallible),
}

impl<Spec, Executable, Value> waymark_vm_interpreter::Interpreter
    for FullSetInterpreter<Spec, Executable, Value>
where
    Spec: waymark_vm_instructions_fullset::Spec,
    Executable: 'static,
    Executable: waymark_vm_executable::FunctionInfo<FunctionId = Spec::FunctionId>,
    Spec: waymark_vm_instructions_coreset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec: waymark_vm_instructions_pureset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec: 'static,
    Spec::FunctionId: Copy,
    Spec::StateId: Copy + Default,
    Spec::ExtCallId: Clone,
    Value: Clone + 'static,
    Value: waymark_vm_interpreter_coreset::Value,
    Value: waymark_vm_interpreter_pureset::Value,
    Spec::ConstValue: Clone + Into<Value>,
{
    type RuntimeView<'r> = RuntimeView<'r, Executable, Spec::FunctionId, Spec::StateId, Value>;
    type Frame = Frame<Spec::FunctionId, Spec::StateId, Promise<Value>>;
    type Instruction = waymark_vm_instructions_fullset::FullSet<Spec>;
    type Error = Error<Spec>;
    type Effect = Effect<Value, Spec::ExtCallId>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Frame<Spec::FunctionId, Spec::StateId, Promise<Value>>,
        instruction: &Self::Instruction,
    ) -> Result<
        ExecutionOutcome<Frame<Spec::FunctionId, Spec::StateId, Promise<Value>>, Self::Effect>,
        Self::Error,
    > {
        Ok(match instruction {
            waymark_vm_instructions_fullset::FullSet::CoreSet(instruction) => {
                let runtime_view = waymark_vm_interpreter_coreset::CoreSetInterpreter::<
                    Spec,
                    Executable,
                    Value,
                >::capture_runtime_view(runtime_view);
                self.core_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::CoreSet)
            }
            waymark_vm_instructions_fullset::FullSet::PureSet(instruction) => {
                #[allow(clippy::let_unit_value)]
                let runtime_view = waymark_vm_interpreter_pureset::PureSetInterpreter::<
                    Spec,
                    Spec::FunctionId,
                    Spec::StateId,
                    Value,
                >::capture_runtime_view(runtime_view);
                self.pure_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::PureSet)
            }
        })
    }
}

impl<Spec: waymark_vm_instructions_fullset::Spec, Executable: 'static, Value: 'static>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, Spec::FunctionId, Spec::StateId, Value>
    for FullSetInterpreter<Spec, Executable, Value>
{
    type RuntimeView<'v> = RuntimeView<'v, Executable, Spec::FunctionId, Spec::StateId, Value>;

    fn capture_runtime_view<'r>(
        view: waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            Spec::FunctionId,
            Spec::StateId,
            Value,
        >,
    ) -> Self::RuntimeView<'r> {
        view
    }
}
