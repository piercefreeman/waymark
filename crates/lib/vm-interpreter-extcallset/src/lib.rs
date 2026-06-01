//! The interpreter for the "extcall" instructions set.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{Frame, RegisterId, RuntimeState};
use waymark_vm_runtime_promise_core::PromiseStateId;

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "extcall" instructions set.
#[derive_where(Default)]
pub struct ExtCallSetInterpreter<Spec, FunctionId, StateId, Value> {
    phantom_data: core::marker::PhantomData<(Spec, FunctionId, StateId, Value)>,
}

/// The runtime view for the [`ExtCallSetInterpreter`].
pub struct RuntimeView<'r, FunctionId, StateId, Value> {
    /// The runtime state access.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value>,
}

/// The effect for the [`ExtCallSetInterpreter`].
#[derive(Debug)]
pub enum Effect<ActionRef, ActionCallArgument> {
    /// Action call invocation is requested.
    ActionCall {
        /// The ID of the promise to resolve with the resulting value when
        /// the action call completes.
        promise_state_id: PromiseStateId,

        /// The action to invoke.
        action_ref: ActionRef,

        /// Args to pass to the action call.
        args: Vec<ActionCallArgument>,
    },

    /// Sleep suspension is requested.
    Sleep {
        /// The ID of the promise to resolve when the sleep finishes.
        promise_state_id: PromiseStateId,

        /// Requested sleep duration.
        duration: NonZeroDuration,
    },
}

fn suspend_frame<FunctionId, StateId, Value>(
    state: &mut RuntimeState<FunctionId, StateId, Value>,
    mut frame: Frame<FunctionId, StateId, Value>,
    dst: RegisterId,
    resume: StateId,
) -> PromiseStateId
where
    Value: waymark_vm_runtime_promise_core::Suspendable,
{
    let promise_state_id = state.promise_states.prepare();
    frame.state = resume;
    frame.exception = None;
    frame.regs.set(dst, Value::from_pending(promise_state_id));
    state.ready.push_front(frame);
    promise_state_id
}

impl<Spec, FunctionId, StateId, Value> waymark_vm_interpreter::Interpreter
    for ExtCallSetInterpreter<Spec, FunctionId, StateId, Value>
where
    Spec: waymark_vm_instructions_extcallset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
            StateId = StateId,
        > + 'static,
    FunctionId: 'static,
    StateId: Copy + 'static,
    Spec::ActionRef: Clone,
    Value: self::value::Value + Clone + 'static,
    Value: waymark_vm_runtime_promise_core::Promisable,
{
    type RuntimeView<'r> = RuntimeView<'r, FunctionId, StateId, Value>;
    type Frame = Frame<FunctionId, StateId, Value>;
    type Instruction = waymark_vm_instructions_extcallset::ExtCallSet<Spec>;
    type Error = Error<Value>;
    type Effect = Effect<Spec::ActionRef, Value::ActionCallArgument>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Frame<FunctionId, StateId, Value>,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Frame<FunctionId, StateId, Value>, Self::Effect>, Self::Error>
    {
        let Self::RuntimeView { state } = runtime_view;

        match instruction {
            waymark_vm_instructions_extcallset::ExtCallSet::ActionCall {
                dst,
                action_ref,
                args,
                resume,
            } => {
                let args = args
                    .iter()
                    .enumerate()
                    .map(|(arg_pos, register)| {
                        let value = frame.regs[*register].clone();

                        value.capture_action_call_argument().map_err(|source| {
                            Error::ActionCall(ActionCallError::ArgumentCapture { arg_pos, source })
                        })
                    })
                    .collect::<Result<_, _>>()?;

                let promise_state_id = suspend_frame(state, frame, *dst, *resume);

                Ok(ExecutionOutcome::ExitFrameWithEffect(Effect::ActionCall {
                    promise_state_id,
                    action_ref: action_ref.clone(),
                    args,
                }))
            }
            waymark_vm_instructions_extcallset::ExtCallSet::Sleep {
                dst,
                duration,
                resume,
            } => {
                let value = &frame.regs[*duration];

                let duration = value
                    .to_sleep_duration()
                    .map_err(|source| Error::Sleep(SleepError::InvalidDuration { source }))?;

                let promise_state_id = suspend_frame(state, frame, *dst, *resume);

                Ok(ExecutionOutcome::ExitFrameWithEffect(Effect::Sleep {
                    promise_state_id,
                    duration,
                }))
            }
        }
    }
}

impl<Spec, Executable, FunctionId, StateId, Value>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, FunctionId, StateId, Value>
    for ExtCallSetInterpreter<Spec, FunctionId, StateId, Value>
{
    type RuntimeView<'v>
        = RuntimeView<'v, FunctionId, StateId, Value>
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        Value: 'v;

    fn capture_runtime_view<'r>(
        view: waymark_vm_runtime_core::FullRuntimeView<'r, Executable, FunctionId, StateId, Value>,
    ) -> Self::RuntimeView<'r> {
        let waymark_vm_runtime_core::FullRuntimeView { state, .. } = view;
        RuntimeView { state }
    }
}
