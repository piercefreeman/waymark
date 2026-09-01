//! The VM runtime.
//!
//! Responsible for the execution loop driving an instruction set intereter
//! and the surface API for executing instructions of the VM bytecode programs.

#![warn(missing_docs)]

pub mod snapshot;
pub mod step;

use std::collections::VecDeque;

use waymark_vm_runtime_core::{
    Frame, FrameKind, PromiseStates, Registers, RuntimeState, SelectStates, SettlePromiseError,
    UnwindStack,
};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// VM runtime.
///
/// Holds an abstract instruction set interpreter, an abstract executable
/// capable of providing instructions from the said instruction set,
/// and the state of the runtime required to drive the execution of
/// the instructions forward.
pub struct Runtime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    interpreter: Interpreter,
    executable: Executable,
    state: RuntimeState<Executable::FunctionId, Executable::StateId, Value>,
}

pub use waymark_vm_runtime_callspec::CallSpec;

/// An error returned when to such function id is defined in the executable.
#[derive(Debug, thiserror::Error)]
#[error("function {function_id:?} is not found in the functions table")]
pub struct FunctionNotFoundError<FunctionId> {
    function_id: FunctionId,
}

impl<Executable, Interpreter, Value> Runtime<Executable, Interpreter, Value>
where
    Interpreter: waymark_vm_interpreter::Interpreter,
    Executable: waymark_vm_executable::FunctionStates,
    Executable: waymark_vm_executable::FunctionInfo,
    Executable::FunctionId: Copy,
    Executable::StateId: Default,
{
    /// Create a new runtime with a conventional entrypoint.
    ///
    /// Conventional entrypoint is a call to a function with a default
    /// function ID (think function 0) with empty arguments.
    pub fn with_conventional_entrypoint(
        interpreter: Interpreter,
        executable: Executable,
    ) -> Result<Self, FunctionNotFoundError<Executable::FunctionId>>
    where
        Executable::FunctionId: Default,
    {
        Self::with_custom_entrypoint(
            interpreter,
            executable,
            CallSpec::<_, Value> {
                func: Executable::FunctionId::default(),
                args: Vec::new(),
            },
        )
    }

    /// Create a new runtime with a custom entrypoint.
    ///
    /// Initialized the top-level frame with a call to the `func` specified
    /// in the `call` and the list of arguments as specified by the `args`
    /// in the `call`. Each argument is converted into the runtime value
    /// type via [`Into`].
    pub fn with_custom_entrypoint<Arg>(
        interpreter: Interpreter,
        executable: Executable,
        call: CallSpec<Executable::FunctionId, Arg>,
    ) -> Result<Self, FunctionNotFoundError<Executable::FunctionId>>
    where
        Arg: Into<Value>,
    {
        let mut ready = VecDeque::new();

        let CallSpec { func, args } = call;

        let num_regs = executable
            .function_num_regs(func)
            .ok_or(FunctionNotFoundError { function_id: func })?;

        let regs = Registers::new_for_fn_call(num_regs, args.into_iter().map(Arg::into));

        ready.push_back(Frame {
            func,
            state: Executable::StateId::default(),
            regs,
            exception: None,
            unwind: UnwindStack::new(),
            kind: FrameKind::TopLevel,
        });

        let state = RuntimeState {
            ready,
            promise_states: PromiseStates::new(),
            select_states: SelectStates::new(),
            effect_counter: waymark_vm_runtime_effect::EffectNumber(0),
        };

        Ok(Self {
            interpreter,
            executable,
            state,
        })
    }
}

/// An error of the [`Runtime::run`] function.
#[derive(Debug, thiserror::Error)]
pub enum RunError<InterpreterError> {
    /// No ready frames to execute.
    ///
    /// Typically either when the program has completed, or when all the frames
    /// are suspended in continuations.
    #[error("no ready frames to execute")]
    NoReadyFrame,

    /// Step execution failed.
    #[error("step: {0}")]
    Step(step::Error<InterpreterError>),
}

/// A type alias shorthand for specifying runtime frames from and executable
/// and a value.
pub type FrameFor<Executable, Value> = Frame<
    <Executable as waymark_vm_executable::Functions>::FunctionId,
    <Executable as waymark_vm_executable::FunctionStates>::StateId,
    Value,
>;

impl<Executable, Interpreter, Value> Runtime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::InstructionsProvider,
    Executable::FunctionId: Copy,
    Executable::StateId: Copy + PartialEq,
    Executable: 'static,
    Interpreter: waymark_vm_interpreter::Interpreter<
            Frame = FrameFor<Executable, Value>,
            Instruction = Executable::Instruction,
        >,
    Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
            Executable,
            Executable::FunctionId,
            Executable::StateId,
            Value,
            RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'r>,
        >,
    Value: 'static,
    // Debug
    Interpreter::Instruction: core::fmt::Debug,
    Value: core::fmt::Debug,
{
    /// Run the VM steps until the next event is encountered.
    ///
    /// Returns the emitted effect paired with its zero-based sequence number.
    /// The counter is embedded in the runtime snapshot so numbering survives
    /// revivals.
    pub fn run(
        &mut self,
    ) -> Result<
        waymark_vm_runtime_effect::EmittedEffect<Interpreter::Effect>,
        RunError<Interpreter::Error>,
    > {
        loop {
            let Some(frame) = self.state.ready.pop_front() else {
                // No frames but also no valid exit either,
                // shouldn't be possible in the valid executing flow.
                return Err(RunError::NoReadyFrame);
            };

            let result = self.step(frame).map_err(RunError::Step)?;
            let effect = match result {
                step::StepOutcome::Effect(effect) => effect,
                step::StepOutcome::Yield => continue,
            };

            let number = self.state.effect_counter;
            self.state.effect_counter =
                waymark_vm_runtime_effect::EffectNumber(self.state.effect_counter.0 + 1);

            return Ok(waymark_vm_runtime_effect::EmittedEffect { effect, number });
        }
    }
}

impl<Executable, Interpreter, Value> Runtime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Value: Clone,
{
    /// Provide an async computation value for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn resolve_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        value: Value::ReadyValue,
    ) -> Result<(), SettlePromiseError<Value::ReadyValue>> {
        self.state
            .resolve_promise(promise_state_id, Value::from_ready(value))
            .map_err(|error| {
                error.map(|new_value| {
                    let Ok(new_value) = new_value.into_ready() else {
                        // We've wrapped this value with `Value::from_ready`
                        // ourselves just a couple lines above.
                        // It is guaranteed to be resolved here.
                        unreachable!();
                    };
                    new_value
                })
            })
    }

    /// Reject an async computation for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn reject_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        exception: waymark_vm_runtime_exception::Exception<Value::ReadyValue>,
    ) -> Result<(), SettlePromiseError<waymark_vm_runtime_exception::Exception<Value::ReadyValue>>>
    {
        let exception = waymark_vm_runtime_exception::Exception {
            type_id: exception.type_id,
            details: Value::from_ready(exception.details),
        };

        self.state
            .reject_promise(promise_state_id, exception)
            .map_err(|error| {
                error.map(|new_value| {
                    let waymark_vm_runtime_exception::Exception { type_id, details } = new_value;

                    let Ok(details) = details.into_ready() else {
                        // We've wrapped this value with `Value::from_ready`
                        // ourselves just a couple lines above.
                        // It is guaranteed to be resolved here.
                        unreachable!();
                    };

                    waymark_vm_runtime_exception::Exception { type_id, details }
                })
            })
    }
}

impl<Executable, Interpreter, Value> Runtime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// Returns `true` if the runtime has ready frames to execute.
    pub fn has_ready_frames(&self) -> bool {
        !self.state.ready.is_empty()
    }

    /// Return an iterator over all currently waiting promise state IDs.
    pub fn waiting_promise_state_ids(
        &self,
    ) -> impl Iterator<Item = waymark_vm_runtime_promise_core::PromiseStateId> + '_ {
        self.state.promise_states.waiting_ids()
    }
}
