//! The interpreter for the "core" instructions set.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{
    Continuation, ExceptionHandlers, Frame, FrameKind, PromiseState, Registers, RuntimeState,
    SettledPromiseState,
};

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "core" instructions set.
#[derive_where(Default)]
pub struct CoreSetInterpreter<Spec, Executable, Value, RaisedException> {
    phantom_data: core::marker::PhantomData<(Spec, Executable, Value, RaisedException)>,
}

/// The runtime view for the [`CoreSetInterpreter`].
pub struct RuntimeView<'r, Executable, FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// The executable access.
    pub executable: &'r Executable,

    /// The runtime state access.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value, RaisedException>,
}

/// The effect for the [`CoreSetInterpreter`].
#[derive(Debug)]
pub enum Effect<Value> {
    /// Program execution is complete.
    Complete(Value),
}

type FrameFor<Spec, Value, RaisedException> = Frame<
    <Spec as waymark_vm_instructions_coreset::Spec>::FunctionId,
    <Spec as waymark_vm_instructions_coreset::Spec>::StateId,
    Value,
    RaisedException,
>;

impl<Spec, Executable, Value, RaisedException> waymark_vm_interpreter::Interpreter
    for CoreSetInterpreter<Spec, Executable, Value, RaisedException>
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
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
    RaisedException: Clone + 'static,
{
    type RuntimeView<'r> =
        RuntimeView<'r, Executable, Spec::FunctionId, Spec::StateId, Value, RaisedException>;
    type Frame = FrameFor<Spec, Value, RaisedException>;
    type Instruction = waymark_vm_instructions_coreset::CoreSet<Spec>;
    type Error = Error<Spec>;
    type Effect = Effect<Value::ReadyValue>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
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
                    exception_handler_blocks: ExceptionHandlers::new(),
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
                            PromiseState::Settled(SettledPromiseState::Resolved(value)) => {
                                Continuation::immediate_resume(
                                    &mut frame,
                                    *resume,
                                    *dst,
                                    value.clone(),
                                );
                                state.ready.push_back(frame);
                                ExecutionOutcome::ExitFrame
                            }
                            PromiseState::Settled(SettledPromiseState::Rejected(exception)) => {
                                Continuation::immediate_raise_exception(
                                    &mut frame,
                                    *resume,
                                    exception.clone(),
                                );
                                state.ready.push_back(frame);
                                ExecutionOutcome::ExitFrame
                            }
                            PromiseState::Waiting(waiters) => {
                                waiters.push(waymark_vm_runtime_core::PromiseWaiter::Await(
                                    Continuation::capture(frame, *resume, *dst),
                                ));
                                ExecutionOutcome::ExitFrame
                            }
                        });
                    }
                    Ok(value) => {
                        frame.regs.set(*dst, Value::from_ready(value.clone()));
                        frame.state = *resume;
                    }
                }
            }
            waymark_vm_instructions_coreset::CoreSet::Select { arms } => {
                if arms.is_empty() {
                    return Err(Error::Select(SelectError::EmptyArms));
                }

                // Scan the arms in the listed order for one whose source
                // has already settled - taking it immediately, mirroring
                // the immediate paths of `Await` - and collect the pending
                // sources meanwhile.
                let mut pending = Vec::with_capacity(arms.len());
                for arm in arms {
                    let promise_state_id = match frame.regs[arm.src].as_ready() {
                        Ok(value) => {
                            // A ready source wins the scan outright.
                            let value = Value::from_ready(value.clone());
                            frame.regs.set(arm.dst, value);
                            frame.state = arm.resume;
                            return Ok(ExecutionOutcome::Continue(frame));
                        }
                        Err(waymark_vm_runtime_promise_core::UnresolvedPromiseError {
                            promise_state_id,
                        }) => promise_state_id,
                    };
                    let promise_state = state
                        .promise_states
                        .get(promise_state_id)
                        .map_err(SelectError::SourcePromiseStateNotFound)
                        .map_err(Error::Select)?;
                    match promise_state {
                        PromiseState::Settled(SettledPromiseState::Resolved(value)) => {
                            Continuation::immediate_resume(
                                &mut frame,
                                arm.resume,
                                arm.dst,
                                value.clone(),
                            );
                            state.ready.push_back(frame);
                            return Ok(ExecutionOutcome::ExitFrame);
                        }
                        PromiseState::Settled(SettledPromiseState::Rejected(exception)) => {
                            Continuation::immediate_raise_exception(
                                &mut frame,
                                arm.resume,
                                exception.clone(),
                            );
                            state.ready.push_back(frame);
                            return Ok(ExecutionOutcome::ExitFrame);
                        }
                        PromiseState::Waiting(_) => {
                            pending.push((promise_state_id, arm.dst, arm.resume));
                        }
                    }
                }

                // Every arm waits: keep the frame aside and plant a claim
                // on each source.
                let handle = state
                    .select_states
                    .insert(Continuation::capture_select(frame));
                for (promise_state_id, dst, resume) in pending {
                    // The scan above just observed these promise states as
                    // waiting, and nothing runs in between.
                    let Ok(promise_state) = state.promise_states.get_mut(promise_state_id) else {
                        unreachable!();
                    };
                    let PromiseState::Waiting(waiters) = promise_state else {
                        unreachable!();
                    };
                    waiters.push(waymark_vm_runtime_core::PromiseWaiter::Select(
                        handle.arm(dst, resume),
                    ));
                }
                return Ok(ExecutionOutcome::ExitFrame);
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
                                waymark_vm_runtime_core::SettlePromiseError::PromiseStateNotFound(
                                    _,
                                ) => ReturnFnCallError::ReturnPromiseNotFound,
                                waymark_vm_runtime_core::SettlePromiseError::AlreadySettled(
                                    _,
                                ) => ReturnFnCallError::ReturnPromiseAlreadySettled,
                            }
                            })
                            .map_err(FnExitError::FnCall)
                            .map_err(Error::Return)?;
                        ExecutionOutcome::ExitFrame
                    }
                    FrameKind::TopLevel => {
                        let val = val
                            .into_ready()
                            .map_err(|(error, _)| FnExitError::TopLevel(error))
                            .map_err(Error::Return)?;
                        ExecutionOutcome::ExitFrameWithEffect(Effect::Complete(val))
                    }
                });
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<'s, 'r, Executable, FunctionId, StateId, Value, RaisedException>
    waymark_vm_runtime_view_capture::CaptureRuntimeView<
        's,
        waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            Value,
            RaisedException,
        >,
    > for RuntimeView<'s, Executable, FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    fn capture_runtime_view(
        source: &'s mut waymark_vm_runtime_core::FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            Value,
            RaisedException,
        >,
    ) -> Self {
        RuntimeView {
            executable: source.executable,
            state: &mut *source.state,
        }
    }
}
