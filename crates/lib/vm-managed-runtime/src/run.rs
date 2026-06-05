use waymark_vm_runtime::step;

use crate::{ActiveRuntime, ManagedRuntime, RunOutcome, RuntimeError, SuspendedRuntime};

impl<Executable, Interpreter, Value> ActiveRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::InstructionsProvider,
    Executable::FunctionId: Copy,
    Executable::StateId: Copy + PartialEq,
    Executable: 'static,
    Interpreter: waymark_vm_interpreter::Interpreter<
            Frame = waymark_vm_runtime::FrameFor<Executable, Value>,
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
    Interpreter::Instruction: core::fmt::Debug,
    Value: core::fmt::Debug,
{
    /// Run the runtime until an effect is produced or the runtime suspends.
    ///
    /// Consumes `self`. On success, returns either:
    /// - [`RunOutcome::Effect`] with the emitted effect and the runtime,
    /// - [`RunOutcome::Suspended`] if no ready frames remain.
    #[allow(clippy::type_complexity)]
    pub fn run(
        mut self,
    ) -> Result<
        RunOutcome<Interpreter::Effect, Executable, Interpreter, Value>,
        RuntimeError<step::Error<Interpreter::Error>, Executable, Interpreter, Value>,
    > {
        debug_assert!(self.runtime.has_ready_frames());
        match self.runtime.run() {
            Ok(effect) => {
                let runtime = ManagedRuntime::from_runtime(self.runtime);
                Ok(RunOutcome::Effect { effect, runtime })
            }
            Err(waymark_vm_runtime::RunError::NoReadyFrame) => {
                debug_assert!(!self.runtime.has_ready_frames());
                Ok(RunOutcome::Suspended {
                    runtime: SuspendedRuntime {
                        runtime: self.runtime,
                    },
                })
            }
            Err(waymark_vm_runtime::RunError::Step(error)) => Err(RuntimeError {
                error,
                runtime: self.runtime,
            }),
        }
    }
}
