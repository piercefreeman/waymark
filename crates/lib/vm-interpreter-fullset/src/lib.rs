//! The interpreter for the "full" instructions set.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::Frame;

pub use self::error::*;
pub use self::value::Value;

type FunctionIdFor<Spec> = <Spec as waymark_vm_instructions_coreset::Spec>::FunctionId;
type StateIdFor<Spec> = <Spec as waymark_vm_instructions_coreset::Spec>::StateId;
type ActionRefFor<Spec> = <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef;

/// An interpreter for the "full" instructions set.
#[derive_where(Default)]
pub struct FullSetInterpreter<Spec: waymark_vm_instructions_fullset::Spec, Executable, Value> {
    /// The coreset interpreter used for core instructions.
    pub core_set: waymark_vm_interpreter_coreset::CoreSetInterpreter<Spec, Executable, Value>,

    /// The extcallset interpreter used for extcall instructions.
    pub extcall_set: waymark_vm_interpreter_extcallset::ExtCallSetInterpreter<
        Spec,
        FunctionIdFor<Spec>,
        StateIdFor<Spec>,
        Value,
    >,

    /// The pureset interpreter used for pure instructions.
    pub pure_set: waymark_vm_interpreter_pureset::PureSetInterpreter<
        Spec,
        FunctionIdFor<Spec>,
        StateIdFor<Spec>,
        Value,
    >,
}

/// The runtime view for the [`FullSetInterpreter`].
pub use waymark_vm_runtime_core::FullRuntimeView as RuntimeView;

/// The effect for the [`FullSetInterpreter`].
#[derive(Debug)]
pub enum Effect<Value, ActionRef, ActionCallArgument> {
    /// An effect produced while executing a coreset instruction.
    CoreSet(waymark_vm_interpreter_coreset::Effect<Value>),

    /// An effect produced while executing an extcallset instruction.
    ExtCallSet(waymark_vm_interpreter_extcallset::Effect<ActionRef, ActionCallArgument>),

    /// An impossible effect produced while executing a pureset instruction.
    PureSet(core::convert::Infallible),
}

impl<Spec, Executable, Value> waymark_vm_interpreter::Interpreter
    for FullSetInterpreter<Spec, Executable, Value>
where
    Spec: waymark_vm_instructions_fullset::Spec,
    Executable: 'static,
    Executable: waymark_vm_executable::FunctionInfo<FunctionId = FunctionIdFor<Spec>>,
    Spec: waymark_vm_instructions_coreset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec: waymark_vm_instructions_extcallset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
            StateId = StateIdFor<Spec>,
        >,
    Spec: waymark_vm_instructions_pureset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec: 'static,
    FunctionIdFor<Spec>: Copy,
    StateIdFor<Spec>: Copy + Default + PartialEq,
    ActionRefFor<Spec>: Clone,
    Value: Clone + 'static,
    Value: waymark_vm_interpreter_coreset::Value,
    Value: waymark_vm_interpreter_extcallset::Value,
    Value: waymark_vm_interpreter_pureset::Value,
    Value: waymark_vm_runtime_exception::FromException<RootValue = Value>,
    Value: waymark_vm_runtime_exception::IntoException<RootValue = Value>,
    Value: for<'a> waymark_vm_interpreter_pureset::value::LoadConst<&'a Spec::ConstValue>,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Value: waymark_vm_runtime_promise_core::Suspendable,
    Value::ReadyValue: Clone,
{
    type RuntimeView<'r> =
        RuntimeView<'r, Executable, FunctionIdFor<Spec>, StateIdFor<Spec>, Value>;
    type Frame = Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>;
    type Instruction = waymark_vm_instructions_fullset::FullSet<Spec>;
    type Error = Error<Spec, Value>;
    type Effect = Effect<Value::ReadyValue, ActionRefFor<Spec>, Value::ActionCallArgument>;

    fn enter_state<'r>(
        &self,
        mut runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        let state_before = frame.state;
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::enter_state(
            &self.core_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::CoreSet)?
        .map_effect(Effect::CoreSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::enter_state(
            &self.extcall_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::ExtCallSet)?
        .map_effect(Effect::ExtCallSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        #[allow(clippy::let_unit_value)]
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::enter_state(
            &self.pure_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::PureSet)?
        .map_effect(Effect::PureSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                Ok(ExecutionOutcome::Continue(next_frame))
            }
            outcome => Ok(outcome),
        }
    }

    fn before_execute<'r>(
        &self,
        mut runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        let state_before = frame.state;
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::before_execute(
            &self.core_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::CoreSet)?
        .map_effect(Effect::CoreSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::before_execute(
            &self.extcall_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::ExtCallSet)?
        .map_effect(Effect::ExtCallSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        #[allow(clippy::let_unit_value)]
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::before_execute(
            &self.pure_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::PureSet)?
        .map_effect(Effect::PureSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                Ok(ExecutionOutcome::Continue(next_frame))
            }
            outcome => Ok(outcome),
        }
    }

    fn after_execute<'r>(
        &self,
        mut runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        let state_before = frame.state;
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::after_execute(
            &self.core_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::CoreSet)?
        .map_effect(Effect::CoreSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::after_execute(
            &self.extcall_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::ExtCallSet)?
        .map_effect(Effect::ExtCallSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                frame = next_frame;
            }
            outcome => return Ok(outcome),
        }

        let state_before = frame.state;
        #[allow(clippy::let_unit_value)]
        let sub_runtime_view =
            waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                &mut runtime_view,
            );
        let outcome = waymark_vm_interpreter::Interpreter::after_execute(
            &self.pure_set,
            sub_runtime_view,
            frame,
        )
        .map_err(Error::PureSet)?
        .map_effect(Effect::PureSet);
        match outcome {
            ExecutionOutcome::Continue(next_frame) if next_frame.state == state_before => {
                Ok(ExecutionOutcome::Continue(next_frame))
            }
            outcome => Ok(outcome),
        }
    }

    fn execute<'r>(
        &self,
        mut runtime_view: Self::RuntimeView<'r>,
        frame: Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>,
        instruction: &Self::Instruction,
    ) -> Result<
        ExecutionOutcome<Frame<FunctionIdFor<Spec>, StateIdFor<Spec>, Value>, Self::Effect>,
        Self::Error,
    > {
        Ok(match instruction {
            waymark_vm_instructions_fullset::FullSet::CoreSet(instruction) => {
                let runtime_view =
                    waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                        &mut runtime_view,
                    );
                self.core_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::CoreSet)
            }
            waymark_vm_instructions_fullset::FullSet::ExtCallSet(instruction) => {
                let runtime_view =
                    waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                        &mut runtime_view,
                    );
                self.extcall_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::ExtCallSet)
            }
            waymark_vm_instructions_fullset::FullSet::PureSet(instruction) => {
                #[allow(clippy::let_unit_value)]
                let runtime_view =
                    waymark_vm_runtime_view_capture::CaptureRuntimeView::capture_runtime_view(
                        &mut runtime_view,
                    );
                self.pure_set
                    .execute(runtime_view, frame, instruction)?
                    .map_effect(Effect::PureSet)
            }
        })
    }
}
