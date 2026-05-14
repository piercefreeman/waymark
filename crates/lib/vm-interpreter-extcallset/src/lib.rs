//! The interpreter for the "extcall" instructions set.

#![warn(missing_docs)]

mod error;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{Frame, Promise, PromiseStateId, RuntimeState};

pub use self::error::*;

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
pub enum Effect<Value, ActionRef> {
    /// Action call invocation is requested.
    ActionCall {
        /// The ID of the promise to resolve with the resulting value when
        /// the action call completes.
        promise_state_id: PromiseStateId,

        /// The action to invoke.
        action_ref: ActionRef,

        /// Args to pass to the action call.
        args: Vec<Value>,
    },
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
    Value: Clone + 'static,
{
    type RuntimeView<'r> = RuntimeView<'r, FunctionId, StateId, Value>;
    type Frame = Frame<FunctionId, StateId, Promise<Value>>;
    type Instruction = waymark_vm_instructions_extcallset::ExtCallSet<Spec>;
    type Error = Error;
    type Effect = Effect<Value, Spec::ActionRef>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionId, StateId, Promise<Value>>,
        instruction: &Self::Instruction,
    ) -> Result<
        ExecutionOutcome<Frame<FunctionId, StateId, Promise<Value>>, Self::Effect>,
        Self::Error,
    > {
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

                        value.require_resolved().map_err(|source| {
                            Error::ExtCall(ExtCallError::UnresolvedPromiseArgument {
                                arg_pos,
                                source,
                            })
                        })
                    })
                    .collect::<Result<_, _>>()?;

                let promise_state_id = state.promise_states.prepare();
                frame.regs.set(*dst, Promise::Pending(promise_state_id));

                frame.state = *resume;

                state.ready.push_front(frame);

                Ok(ExecutionOutcome::ExitFrameWithEffect(Effect::ActionCall {
                    promise_state_id,
                    action_ref: action_ref.clone(),
                    args,
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
