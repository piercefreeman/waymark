//! The interpreter for the "excset" instructions set.
//!
//! This crate executes exception-oriented VM instructions and defines the
//! value conversions needed to inspect exception values.
//! [`ExcSetInterpreter`] evaluates exception type checks and copies exception
//! details payloads into destination registers.

#![warn(missing_docs)]

mod error;
pub mod value;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{Frame, FrameKind, RuntimeState};

pub use self::error::*;
pub use self::value::Value;

/// An interpreter for the "excset" instructions set.
#[derive_where(Default)]
pub struct ExcSetInterpreter<Spec, Executable, FunctionId, StateId, Value> {
    phantom_data: core::marker::PhantomData<(Spec, Executable, FunctionId, StateId, Value)>,
}

/// The runtime view for the [`ExcSetInterpreter`].
pub struct RuntimeView<'r, Executable, FunctionId, StateId, Value> {
    /// The executable access.
    pub executable: &'r Executable,

    /// The runtime state access.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value>,
}

/// The effect for the [`ExcSetInterpreter`].
#[derive(Debug)]
pub enum Effect<Value> {
    /// Program execution is complete.
    Complete(Result<Value, Value>),
}

impl<Value> From<Result<Value, Value>> for Effect<Value> {
    fn from(value: Result<Value, Value>) -> Self {
        Self::Complete(value)
    }
}

type Outcome<Value, FunctionId, StateId> = ExecutionOutcome<
    Frame<FunctionId, StateId, Value>,
    Effect<<Value as waymark_vm_runtime_promise_core::Resolvable>::ReadyValue>,
>;

impl<Spec, Executable, FunctionId, StateId, Value>
    ExcSetInterpreter<Spec, Executable, FunctionId, StateId, Value>
where
    Executable: 'static,
    Spec: waymark_vm_instructions_excset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    FunctionId: Copy + 'static,
    StateId: Copy + 'static,
    Value: self::Value,
    Value: Clone,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Value: 'static,
{
    fn raise_from_register(
        state: &mut RuntimeState<FunctionId, StateId, Value>,
        frame: Frame<FunctionId, StateId, Value>,
        src: waymark_vm_runtime_core::RegisterId,
    ) -> Result<Outcome<Value, FunctionId, StateId>, RaiseError> {
        let val = frame.regs[src].clone();

        Ok(match frame.kind {
            FrameKind::FnCall { ret } => {
                state
                    .resolve_promise(ret, val)
                    .map_err(|error| match error {
                        waymark_vm_runtime_core::ResolvePromiseError::PromiseStateNotFound(_) => {
                            RaiseFnCallError::ReturnPromiseNotFound
                        }
                        waymark_vm_runtime_core::ResolvePromiseError::AlreadyResolved(_) => {
                            RaiseFnCallError::ReturnPromiseAlreadyResolved
                        }
                    })
                    .map_err(RaiseError::FnCall)?;
                ExecutionOutcome::ExitFrame
            }
            FrameKind::TopLevel => {
                let val = val
                    .into_ready()
                    .map_err(|(error, _)| RaiseError::TopLevel(error))?;
                ExecutionOutcome::ExitFrameWithEffect(Effect::Complete(Err(val)))
            }
        })
    }
}

impl<Spec, Executable, FunctionId, StateId, Value> waymark_vm_interpreter::Interpreter
    for ExcSetInterpreter<Spec, Executable, FunctionId, StateId, Value>
where
    Executable: 'static,
    Spec: waymark_vm_instructions_excset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    FunctionId: Copy + 'static,
    StateId: Copy + 'static,
    Value: self::Value,
    Value: Clone,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Value: 'static,
{
    type RuntimeView<'r> = RuntimeView<'r, Executable, FunctionId, StateId, Value>;
    type Frame = Frame<FunctionId, StateId, Value>;
    type Instruction = waymark_vm_instructions_excset::ExcSet<Spec>;
    type Error = Error;
    type Effect = Effect<Value::ReadyValue>;

    fn execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        mut frame: Frame<FunctionId, StateId, Value>,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Frame<FunctionId, StateId, Value>, Self::Effect>, Self::Error>
    {
        let RuntimeView {
            executable: _,
            state,
        } = runtime_view;

        match instruction {
            waymark_vm_instructions_excset::ExcSet::IsException {
                dst,
                src,
                exception_type_id,
            } => {
                let val = &frame.regs[*src];
                let is_exception = match val.as_exception() {
                    Err(waymark_vm_runtime_exception::NotAnExceptionError) => false,
                    Ok(exception) => match exception_type_id {
                        None => true,
                        Some(exception_type_id) => {
                            let type_id = &frame.regs[*exception_type_id];
                            let type_id =
                                type_id.as_exception_type_id().map_err(Error::IsException)?;

                            exception.type_id == type_id
                        }
                    },
                };

                frame.regs.set(*dst, Value::from_is_exception(is_exception));
            }

            waymark_vm_instructions_excset::ExcSet::ExceptionDetails { dst, src } => {
                let val = &frame.regs[*src];
                let exception = val.as_exception().map_err(Error::ExceptionDetails)?;

                frame
                    .regs
                    .set(*dst, Value::from_exception_details(&exception.details))
            }

            waymark_vm_instructions_excset::ExcSet::Raise { src } => {
                return Self::raise_from_register(state, frame, *src).map_err(Error::Raise);
            }
        }

        Ok(ExecutionOutcome::Continue(frame))
    }
}

impl<Spec, Executable, FunctionId, StateId, Value>
    waymark_vm_runtime_core::CaptureRuntimeView<Executable, FunctionId, StateId, Value>
    for ExcSetInterpreter<Spec, Executable, FunctionId, StateId, Value>
{
    type RuntimeView<'v>
        = RuntimeView<'v, Executable, FunctionId, StateId, Value>
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        Value: 'v;

    fn capture_runtime_view<'r>(
        view: waymark_vm_runtime_core::FullRuntimeView<'r, Executable, FunctionId, StateId, Value>,
    ) -> Self::RuntimeView<'r> {
        let waymark_vm_runtime_core::FullRuntimeView { executable, state } = view;
        RuntimeView { executable, state }
    }
}
