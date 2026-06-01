//! A single step execution logic.

use crate::{FrameFor, Runtime};

pub(crate) enum StepOutcome<Effect> {
    Effect(Effect),
    Yield,
}

/// An error of the [`Runtime::step`] function.
#[derive(Debug, thiserror::Error)]
pub enum Error<InterpreterError> {
    /// An interpreter has failed executing an instruction.
    #[error("execution: {0}")]
    Execution(InterpreterError),

    /// The current frame points at a function state that does not exist.
    #[error("current frame references a missing function state")]
    InvalidState,

    /// No instructions left to execute.
    ///
    /// This is typically a mistake in the bytecode, as we should never
    /// run out of instructions in a valid program - each state is supposed
    /// to finish with a terminal that would consume a frame before we have
    /// a chance to reach past the end of the instructions sequence.
    #[error("no instructions left to execute")]
    NoInstructions,
}

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
    Interpreter: for<'v> waymark_vm_runtime_core::CaptureRuntimeView<
            Executable,
            Executable::FunctionId,
            Executable::StateId,
            Value,
            RuntimeView<'v> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'v>,
        >,
    Value: 'static,
    Interpreter::Instruction: core::fmt::Debug,
    Value: core::fmt::Debug,
{
    /// Consume and execute a frame till a side-effect is encountered.
    pub(crate) fn step(
        &mut self,
        mut frame: FrameFor<Executable, Value>,
    ) -> Result<StepOutcome<Interpreter::Effect>, Error<Interpreter::Error>> {
        let Self {
            executable,
            interpreter,
            state,
        } = self;

        'state_loop: loop {
            let current_state = frame.state;
            let full_runtime_view = waymark_vm_runtime_core::FullRuntimeView { executable, state };
            let runtime_view = Interpreter::capture_runtime_view(full_runtime_view);
            let outcome = interpreter
                .enter_state(runtime_view, frame)
                .map_err(Error::Execution)?;

            match outcome {
                waymark_vm_interpreter::ExecutionOutcome::Continue(next_frame) => {
                    frame = next_frame;
                    if frame.state != current_state {
                        continue 'state_loop;
                    }
                }
                waymark_vm_interpreter::ExecutionOutcome::ExitFrame => {
                    return Ok(StepOutcome::Yield);
                }
                waymark_vm_interpreter::ExecutionOutcome::ExitFrameWithEffect(effect) => {
                    return Ok(StepOutcome::Effect(effect));
                }
            }

            let instructions = executable
                .function_state_instructions(frame.func, frame.state)
                .ok_or(Error::InvalidState)?;

            for instruction in instructions {
                tracing::trace!(
                    regs = ?frame.regs,
                    frame_kind = ?frame.kind,
                    ?instruction,
                    "step instruction execution"
                );

                let current_state = frame.state;
                let full_runtime_view =
                    waymark_vm_runtime_core::FullRuntimeView { executable, state };
                let runtime_view = Interpreter::capture_runtime_view(full_runtime_view);
                let outcome = interpreter
                    .execute(runtime_view, frame, instruction)
                    .map_err(Error::Execution)?;

                match outcome {
                    waymark_vm_interpreter::ExecutionOutcome::Continue(next_frame) => {
                        let state_changed = next_frame.state != current_state;
                        frame = next_frame;

                        if state_changed {
                            continue 'state_loop;
                        }
                    }
                    waymark_vm_interpreter::ExecutionOutcome::ExitFrame => {
                        // Done with this frame, yield to schedule next work.
                        return Ok(StepOutcome::Yield);
                    }
                    waymark_vm_interpreter::ExecutionOutcome::ExitFrameWithEffect(effect) => {
                        // Done with this frame, but also have an effect to emit.
                        return Ok(StepOutcome::Effect(effect));
                    }
                };
            }

            return Err(Error::NoInstructions);
        }
    }
}
