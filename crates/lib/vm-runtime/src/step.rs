use waymark_vm_runtime_core::{Frame, Promise, RuntimeState};

use crate::Runtime;

pub(crate) enum StepOutcome<Effect> {
    Effect(Effect),
    Yield,
}

#[derive(Debug, thiserror::Error)]
pub enum StepError<ExecutionError> {
    Execution(ExecutionError),
    NoInstructions,
}

pub trait InstructionsProvider {
    type Instruction;

    fn function_state_instructions(
        &self,
        function_id: waymark_vm_bytecode::FunctionId,
        state_id: waymark_vm_bytecode::StateId,
    ) -> Option<impl IntoIterator<Item = &Self::Instruction> + '_>;
}

impl<Interpreter, Executable, Value> Runtime<Interpreter, Executable, Value>
where
    Interpreter: waymark_vm_interpreter::Interpreter<Frame = Frame<Promise<Value>>>,
    for<'r> Interpreter::RuntimeView<'r>: From<(&'r Executable, &'r mut RuntimeState<Value>)>,
    Executable: self::InstructionsProvider<Instruction = Interpreter::Instruction>,
{
    /// Consume and execute a frame till a side-effect is encountered.
    pub(crate) fn step(
        &mut self,
        mut frame: Frame<Promise<Value>>,
    ) -> Result<StepOutcome<Interpreter::Effect>, StepError<Interpreter::Error>> {
        let Self {
            executable,
            interpreter,
            state,
        } = self;

        let instructions = executable
            .function_state_instructions(frame.func, frame.state)
            .unwrap();

        for instruction in instructions {
            let runtime_view: (&Executable, &mut RuntimeState<Value>) = (executable, state);
            let runtime_view = runtime_view.into();
            let outcome = interpreter
                .execute(runtime_view, frame, instruction)
                .map_err(StepError::Execution)?;

            frame = match outcome {
                waymark_vm_interpreter::ExecutionOutcome::Continue(frame) => {
                    // Update the frame and continue executing it.
                    frame
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

        Err(StepError::NoInstructions)
    }
}
