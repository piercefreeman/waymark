//! The interpreter for the "exception" instructions set.
//!
//! Owns everything exceptional about a frame: the handler-block stack,
//! raising, and the unwind. The unwind runs from the hooks, after every
//! instruction of any set and on every state entry, so an exception parked
//! on the frame by anyone - a `Raise`, an operation failure in another set,
//! a rejected promise observed by an await - takes the same path: the
//! innermost matching handler, else the frame exits, rejecting its call
//! promise or, at the top level, emitting the unhandled exception.

#![warn(missing_docs)]

mod error;
mod load_const_exception;
mod load_const_exception_pattern;

use derive_where::derive_where;
use waymark_vm_interpreter::ExecutionOutcome;
use waymark_vm_runtime_core::{Frame, FrameKind, RuntimeState};

pub use self::error::*;
pub use self::load_const_exception::*;
pub use self::load_const_exception_pattern::*;

/// An interpreter for the "exception" instructions set.
///
/// `RaisedException` is the runtime form of an exception, the way `Value`
/// is the runtime form of a value: never in the bytecode, which carries the
/// spec's const exception and const pattern instead.
#[derive_where(Default)]
pub struct ExcSetInterpreter<Spec, FunctionId, Value, RaisedException> {
    phantom_data: core::marker::PhantomData<(Spec, FunctionId, Value, RaisedException)>,
}

/// The runtime view for the [`ExcSetInterpreter`].
pub struct RuntimeView<'r, FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// The runtime state access.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value, RaisedException>,
}

/// The effect for the [`ExcSetInterpreter`].
#[derive(Debug)]
pub enum Effect<RaisedException> {
    /// Program execution terminated with an unhandled exception.
    UnhandledException(RaisedException),
}

type StateIdFor<Spec> = <Spec as waymark_vm_instructions_excset::Spec>::StateId;
type FrameFor<Spec, FunctionId, Value, RaisedException> =
    Frame<FunctionId, StateIdFor<Spec>, Value, RaisedException>;
type ErrorFor<Value, RaisedException> = Error<
    RaiseError<
        <Value as waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>>::Error,
    >,
    <Value as waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>>::Error,
>;

impl<Spec, FunctionId, Value, RaisedException>
    ExcSetInterpreter<Spec, FunctionId, Value, RaisedException>
where
    Spec: waymark_vm_instructions_excset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec::StateId: Copy,
    Value: Clone,
    Value: waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>,
    Value: waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>,
    RaisedException: waymark_vm_runtime_exception::Match,
    RaisedException: Clone,
{
    /// Deliver the frame's pending exception, if any: to the innermost
    /// handler that catches it, or out of the frame.
    #[expect(
        clippy::type_complexity,
        reason = "the outcome spells out the frame and the effect in full on purpose"
    )]
    fn unwind(
        state: &mut RuntimeState<FunctionId, Spec::StateId, Value, RaisedException>,
        mut frame: FrameFor<Spec, FunctionId, Value, RaisedException>,
    ) -> Result<
        ExecutionOutcome<
            FrameFor<Spec, FunctionId, Value, RaisedException>,
            Effect<RaisedException>,
        >,
        ErrorFor<Value, RaisedException>,
    > {
        let Some(exception) = frame.exception.take() else {
            return Ok(ExecutionOutcome::Continue(frame));
        };

        if let Some(handler) = frame
            .exception_handler_blocks
            .take_matching(|pattern| exception.matches(pattern))
        {
            if let Some(dst) = handler.exception_dst {
                let value = Value::from_raised(exception).map_err(Error::Capture)?;
                frame.regs.set(dst, value);
            }
            frame.state = handler.handler_state;
            return Ok(ExecutionOutcome::Continue(frame));
        }

        Ok(match frame.kind {
            FrameKind::FnCall { ret } => {
                state
                    .reject_promise(ret, exception)
                    .map_err(|error| match error {
                        waymark_vm_runtime_core::SettlePromiseError::PromiseStateNotFound(_) => {
                            UnwindError::ReturnPromiseNotFound
                        }
                        waymark_vm_runtime_core::SettlePromiseError::AlreadySettled(_) => {
                            UnwindError::ReturnPromiseAlreadySettled
                        }
                    })
                    .map_err(Error::Unwind)?;
                ExecutionOutcome::ExitFrame
            }
            FrameKind::TopLevel => {
                ExecutionOutcome::ExitFrameWithEffect(Effect::UnhandledException(exception))
            }
        })
    }
}

impl<Spec, FunctionId, Value, RaisedException> waymark_vm_interpreter::Interpreter
    for ExcSetInterpreter<Spec, FunctionId, Value, RaisedException>
where
    Spec: waymark_vm_instructions_excset::Spec<RegisterId = waymark_vm_runtime_core::RegisterId>,
    Spec::StateId: Copy,
    FunctionId: 'static,
    Value: 'static,
    Value: Clone,
    Value: waymark_vm_runtime_exception::ValueToRaisedException<RaisedException>,
    Value: waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException>,
    RaisedException: 'static,
    RaisedException: waymark_vm_runtime_exception::Match,
    RaisedException: Clone,
    RaisedException: for<'a> LoadConstException<&'a Spec::ConstException>,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>:
        for<'a> LoadConstExceptionPattern<&'a Spec::ConstExceptionPattern>,
{
    type RuntimeView<'r> = RuntimeView<'r, FunctionId, Spec::StateId, Value, RaisedException>;
    type Frame = FrameFor<Spec, FunctionId, Value, RaisedException>;
    type Instruction = waymark_vm_instructions_excset::ExcSet<Spec>;
    type Error = ErrorFor<Value, RaisedException>;
    type Effect = Effect<RaisedException>;

    fn enter_state<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        Self::unwind(runtime_view.state, frame)
    }

    fn after_execute<'r>(
        &self,
        runtime_view: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        Self::unwind(runtime_view.state, frame)
    }

    fn execute<'r>(
        &self,
        _runtime_view: Self::RuntimeView<'r>,
        mut frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        match instruction {
            waymark_vm_instructions_excset::ExcSet::PushExceptionHandlers { handlers } => {
                let handlers = handlers
                    .iter()
                    .map(|handler| {
                        handler.map_pattern_ref_cloned(|pattern| {
                            waymark_vm_runtime_exception::MatchPatternOf::<RaisedException>::load_const_exception_pattern(pattern)
                        })
                    })
                    .collect();
                frame.exception_handler_blocks.push(handlers);
            }
            waymark_vm_instructions_excset::ExcSet::PopExceptionHandlers { count } => {
                frame
                    .exception_handler_blocks
                    .pop(*count)
                    .map_err(ExceptionHandlersError::Pop)
                    .map_err(Error::ExceptionHandlers)?;
            }
            waymark_vm_instructions_excset::ExcSet::Raise { src } => {
                let value = frame
                    .regs
                    .get(*src)
                    .ok_or(RaiseError::MissingSource { register: *src })
                    .map_err(Error::Raise)?
                    .clone();
                let exception = value
                    .into_raised()
                    .map_err(RaiseError::Value)
                    .map_err(Error::Raise)?;
                frame.raise_exception(exception);
            }
            waymark_vm_instructions_excset::ExcSet::RaiseConst { exception } => {
                frame.raise_exception(RaisedException::load_const_exception(exception));
            }
        }

        // A raise leaves the exception on the frame; the after-execute hook
        // unwinds it.
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
    > for RuntimeView<'s, FunctionId, StateId, Value, RaisedException>
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
            state: &mut *source.state,
        }
    }
}
