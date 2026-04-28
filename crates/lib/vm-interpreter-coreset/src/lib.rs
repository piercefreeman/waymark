use index_type::IndexType;
use waymark_vm_interpreter::ExecutionOutcome;

use waymark_vm_runtime_core::{
    Continuation, Frame, FrameKind, Promise, PromiseState, PromiseStateId, Registers,
    ResolvingAlreadyResolvedPromiseError, RuntimeState, UnresolvedPromiseError,
};

pub struct CoreSetInterpreter<Spec, Executable, Value> {
    phantom_data: core::marker::PhantomData<(Spec, Executable, Value)>,
}

impl<Spec, Executable, Value> Default for CoreSetInterpreter<Spec, Executable, Value> {
    fn default() -> Self {
        Self {
            phantom_data: Default::default(),
        }
    }
}

pub struct RuntimeView<'r, Executable, Value> {
    pub executable: &'r Executable,
    pub state: &'r mut RuntimeState<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error<Spec: waymark_vm_instructions_coreset::Spec> {
    #[error("extcall: {0}")]
    ExtCall(#[source] ExtCallError),

    #[error("return: {0}")]
    Return(#[source] ReturnError),

    #[error("jump if: {0}")]
    JumpIf(#[source] JumpIfError),

    #[error("function not found: {0}")]
    FunctionNotFound(#[source] FunctionNotFoundError<Spec::FunctionId>),
}

#[derive(Debug, thiserror::Error)]
#[error("function {function_id} not found in the executable")]
pub struct FunctionNotFoundError<FunctionId> {
    pub function_id: FunctionId,
}

#[derive(Debug, thiserror::Error)]
pub enum JumpIfError {
    #[error("unresolved conditional value: {0}")]
    UnresolvedConditionPromise(#[source] UnresolvedPromiseError),

    #[error("condition check: {0}")]
    ConditionCheck(#[source] NotAConditionalError),
}

#[derive(Debug, thiserror::Error)]
pub enum ExtCallError {
    #[error("unresolved promise argument at position {arg_pos}: {source}")]
    UnresolvedPromiseArgument {
        arg_pos: usize,
        #[source]
        source: UnresolvedPromiseError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ReturnError {
    #[error("from fn call: {0}")]
    FnCall(#[source] ResolvingAlreadyResolvedPromiseError),

    #[error("toplevel: {0}")]
    TopLevel(#[source] UnresolvedPromiseError),
}

pub enum Effect<Value, ExtCallId> {
    /// Program execution is complete.
    Complete(Value),

    /// Extcall invocation is requested.
    ExtCall {
        /// The ID of the promise to resolve with the resulting value when
        /// the extcall completes.
        promise_state_id: PromiseStateId,

        /// The ID of the extcall to invoke from the extcall table.
        extcall_id: ExtCallId,

        /// Args to pass to the extcall.
        args: Vec<Value>,
    },

    /// Runtime suspension is requested.
    Suspend {
        /// The ID of the promise that must resolve before we can resume
        /// the execution.
        waiting_on: PromiseStateId,
    },
}

pub trait FunctionInfo<FunctionId: Copy> {
    fn function_num_regs(&self, function_id: FunctionId) -> Option<usize>;
}

#[derive(Debug, thiserror::Error)]
#[error("the value is not a conditional")]
pub struct NotAConditionalError;

pub trait ShouldJump {
    fn should_jump(&self) -> Result<bool, NotAConditionalError>;
}

impl<Spec, Executable, Value> waymark_vm_interpreter::Interpreter
    for CoreSetInterpreter<Spec, Executable, Value>
where
    Executable: 'static,
    Executable: FunctionInfo<Spec::FunctionId>,
    Spec: waymark_vm_instructions_coreset::Spec<
            FunctionId = waymark_vm_bytecode::FunctionId,
            StateId = waymark_vm_bytecode::StateId,
            RegisterId = waymark_vm_runtime_core::RegisterId,
        >,
    Value: ShouldJump,
    Value: Clone,
    Value: 'static,
    Spec::ExtCallId: Clone,
{
    type RuntimeView<'r> = RuntimeView<'r, Executable, Value>;
    type Frame = Frame<Promise<Value>>;
    type Instruction = waymark_vm_instructions_coreset::CoreSet<Spec>;
    type Error = Error<Spec>;
    type Effect = Effect<Value, Spec::ExtCallId>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<Promise<Value>>,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Frame<Promise<Value>>, Self::Effect>, Self::Error> {
        let Self::RuntimeView { executable, state } = runtime_view;

        match instruction {
            waymark_vm_instructions_coreset::CoreSet::Call {
                dst,
                function_id,
                args,
            } => {
                let promise_id = state.promise_states.prepare();
                frame.regs[*dst] = Promise::Pending(promise_id);

                let num_regs = executable
                    .function_num_regs(*function_id)
                    .ok_or(FunctionNotFoundError {
                        function_id: *function_id,
                    })
                    .map_err(Error::FunctionNotFound)?;

                let regs = Registers::new_for_fn_call(
                    num_regs,
                    args.iter().map(|arg| frame.regs[*arg].clone()),
                );

                let new_frame = Frame {
                    func: *function_id,
                    state: IndexType::ZERO,
                    regs,
                    kind: FrameKind::FnCall { ret: promise_id },
                };

                state.ready.push_back(new_frame);
            }
            waymark_vm_instructions_coreset::CoreSet::ExtCall {
                dst,
                extcall_id,
                args,
            } => {
                let promise_state_id = state.promise_states.prepare();
                frame.regs[*dst] = Promise::Pending(promise_state_id);

                let args = args
                    .iter()
                    .map(|r| frame.regs[*r].clone())
                    .enumerate()
                    .map(|(arg_pos, value)| {
                        value.require_resolved().map_err(|err| {
                            ExtCallError::UnresolvedPromiseArgument {
                                arg_pos,
                                source: err,
                            }
                        })
                    })
                    .collect::<Result<_, _>>()
                    .map_err(Error::ExtCall)?;

                state.ready.push_front(frame);

                return Ok(ExecutionOutcome::ExitFrameWithEffect(Effect::ExtCall {
                    promise_state_id,
                    extcall_id: extcall_id.clone(),
                    args,
                }));
            }
            waymark_vm_instructions_coreset::CoreSet::Await { dst, src, resume } => {
                match &frame.regs[*src] {
                    Promise::Pending(promise_state_id) => {
                        let promise_state_id = *promise_state_id;
                        let promise_state = &mut state.promise_states[promise_state_id];
                        return Ok(match promise_state {
                            PromiseState::Ready(value) => {
                                Continuation::immediate_resume(
                                    &mut frame,
                                    *dst,
                                    *resume,
                                    value.clone(),
                                );
                                state.ready.push_back(frame);
                                ExecutionOutcome::ExitFrame
                            }
                            PromiseState::Waiting(continuations) => {
                                continuations.push(Continuation::capture(frame, *dst, *resume));
                                ExecutionOutcome::ExitFrameWithEffect(Effect::Suspend {
                                    waiting_on: promise_state_id,
                                })
                            }
                        });
                    }
                    Promise::Resolved(value) => {
                        frame.regs[*dst] = Promise::Resolved(value.clone());
                    }
                }
            }
            waymark_vm_instructions_coreset::CoreSet::Jump { target_state } => {
                frame.state = *target_state;
            }
            waymark_vm_instructions_coreset::CoreSet::JumpIf { target_state, cond } => {
                let value = frame.regs[*cond].clone();
                let value = value
                    .require_resolved_ref()
                    .map_err(JumpIfError::UnresolvedConditionPromise)
                    .map_err(Error::JumpIf)?;

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

                match frame.kind {
                    FrameKind::FnCall { ret } => {
                        state
                            .resolve_promise(ret, val)
                            .map_err(ReturnError::FnCall)
                            .map_err(Error::Return)?;
                        return Ok(ExecutionOutcome::Continue(frame));
                    }
                    FrameKind::TopLevel => {
                        let val = val
                            .require_resolved()
                            .map_err(ReturnError::TopLevel)
                            .map_err(Error::Return)?;
                        return Ok(ExecutionOutcome::ExitFrameWithEffect(Effect::Complete(val)));
                    }
                }
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}
