//! The interpreter for the "core" instructions set.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{
    Continuation, Frame, FrameKind, PromiseState, Registers, RuntimeState,
};

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "core" instructions set.
#[derive_where(Default)]
pub struct CoreSetInterpreter<Spec, Executable, Value> {
    phantom_data: core::marker::PhantomData<(Spec, Executable, Value)>,
}

/// The runtime view for the [`CoreSetInterpreter`].
pub struct RuntimeView<'r, Executable, FunctionId, StateId, Value> {
    /// The executable access.
    pub executable: &'r Executable,

    /// The runtime state access.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value>,
}

/// The effect for the [`CoreSetInterpreter`].
#[derive(Debug)]
pub enum Effect<Value> {
    /// Program execution is complete.
    Complete(Value),
}

impl<Spec, Executable, Value> waymark_vm_interpreter::Interpreter
    for CoreSetInterpreter<Spec, Executable, Value>
where
    Executable: 'static,
    Executable: waymark_vm_executable::FunctionInfo<FunctionId = Spec::FunctionId>,
    Spec: waymark_vm_instructions_coreset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec::FunctionId: Copy,
    Spec::StateId: Copy + Default,
    Value: self::Value,
    Value: Clone,
    Value: waymark_vm_runtime_promise_core::Suspendable,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Value::ReadyValue: Clone,
    Value: 'static,
{
    type RuntimeView<'r> = RuntimeView<'r, Executable, Spec::FunctionId, Spec::StateId, Value>;
    type Frame = Frame<Spec::FunctionId, Spec::StateId, Value>;
    type Instruction = waymark_vm_instructions_coreset::CoreSet<Spec>;
    type Error = Error<Spec>;
    type Effect = Effect<Value::ReadyValue>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<Spec::FunctionId, Spec::StateId, Value>,
        instruction: &Self::Instruction,
    ) -> Result<
        ExecutionOutcome<Frame<Spec::FunctionId, Spec::StateId, Value>, Self::Effect>,
        Self::Error,
    > {
        let Self::RuntimeView { executable, state } = runtime_view;

        match instruction {
            waymark_vm_instructions_coreset::CoreSet::Call {
                dst,
                function_id,
                args,
            } => {
                let num_regs = executable
                    .function_num_regs(*function_id)
                    .ok_or(CallError::FunctionNotFound {
                        function_id: *function_id,
                    })
                    .map_err(Error::Call)?;

                let args = args
                    .iter()
                    .map(|arg| frame.regs[*arg].capture_call_argument())
                    .collect::<Vec<_>>();

                let promise_state_id = state.promise_states.prepare();
                frame.regs.set(*dst, Value::from_pending(promise_state_id));

                let regs = Registers::new_for_fn_call(num_regs, args);

                let new_frame = Frame {
                    func: *function_id,
                    state: Spec::StateId::default(),
                    regs,
                    exception: None,
                    kind: FrameKind::FnCall {
                        ret: promise_state_id,
                    },
                };

                state.ready.push_back(new_frame);
            }
            waymark_vm_instructions_coreset::CoreSet::Await { dst, src, resume } => {
                let value = &frame.regs[*src];
                match value.as_ready() {
                    Err(waymark_vm_runtime_promise_core::UnresolvedPromiseError {
                        promise_state_id,
                    }) => {
                        let promise_state = state
                            .promise_states
                            .get_mut(promise_state_id)
                            .map_err(AwaitError::SourcePromiseStateNotFound)
                            .map_err(Error::Await)?;
                        return Ok(match promise_state {
                            PromiseState::Resolved(value) => {
                                Continuation::immediate_resume(
                                    &mut frame,
                                    *resume,
                                    *dst,
                                    value.clone(),
                                );
                                state.ready.push_back(frame);
                                ExecutionOutcome::ExitFrame
                            }
                            PromiseState::Rejected(exception) => {
                                Continuation::immediate_raise_exception(
                                    &mut frame,
                                    *resume,
                                    exception.clone(),
                                );
                                state.ready.push_back(frame);
                                ExecutionOutcome::ExitFrame
                            }
                            PromiseState::Waiting(continuations) => {
                                continuations.push(Continuation::capture(frame, *resume, *dst));
                                ExecutionOutcome::ExitFrame
                            }
                        });
                    }
                    Ok(value) => frame.regs.set(*dst, Value::from_ready(value.clone())),
                }
            }
            waymark_vm_instructions_coreset::CoreSet::Jump { target_state } => {
                frame.state = *target_state;
            }
            waymark_vm_instructions_coreset::CoreSet::JumpIf { target_state, cond } => {
                let value = &frame.regs[*cond];

                let should_jump = value
                    .should_jump()
                    .map_err(JumpIfError::ConditionCheck)
                    .map_err(Error::JumpIf)?;

                if should_jump {
                    frame.state = *target_state;
                }
            }
            waymark_vm_instructions_coreset::CoreSet::Return { src } => {
                let val = frame.regs[*src].clone();

                return Ok(match frame.kind {
                    FrameKind::FnCall { ret } => {
                        state
                            .resolve_promise(ret, val)
                            .map_err(|error| {
                                match error {
                                waymark_vm_runtime_core::ResolvePromiseError::PromiseStateNotFound(
                                    _,
                                ) => ReturnFnCallError::ReturnPromiseNotFound,
                                waymark_vm_runtime_core::ResolvePromiseError::AlreadyResolved(
                                    _,
                                ) => ReturnFnCallError::ReturnPromiseAlreadyResolved,
                            }
                            })
                            .map_err(ReturnError::FnCall)
                            .map_err(Error::Return)?;
                        ExecutionOutcome::ExitFrame
                    }
                    FrameKind::TopLevel => {
                        let val = val
                            .into_ready()
                            .map_err(|(error, _)| ReturnError::TopLevel(error))
                            .map_err(Error::Return)?;
                        ExecutionOutcome::ExitFrameWithEffect(Effect::Complete(val))
                    }
                });
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<Spec, Executable: 'static, Value: 'static>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, Spec::FunctionId, Spec::StateId, Value>
    for CoreSetInterpreter<Spec, Executable, Value>
where
    Spec: waymark_vm_instructions_coreset::Spec,
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
        let waymark_vm_runtime_core::FullRuntimeView { executable, state } = view;
        RuntimeView { executable, state }
    }
}
