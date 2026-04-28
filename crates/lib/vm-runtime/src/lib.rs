mod step;

use std::collections::VecDeque;

use index_type::IndexType;
use waymark_vm_runtime_core::{
    Frame, FrameKind, Promise, PromiseStateId, PromiseStates, Registers,
    ResolvingAlreadyResolvedPromiseError, RuntimeState,
};

pub struct Runtime<Interpreter, Executable, Value> {
    interpreter: Interpreter,
    executable: Executable,
    state: RuntimeState<Value>,
}

pub struct CallSpec<Value> {
    pub func: waymark_vm_bytecode::FunctionId,
    pub args: Vec<Value>,
}

pub trait InstructionsProvider {
    type Instruction;

    fn function_state_instructions(
        &self,
        function_id: waymark_vm_bytecode::FunctionId,
        state_id: waymark_vm_bytecode::StateId,
    ) -> Option<impl IntoIterator<Item = &Self::Instruction> + '_>;
}

pub trait ExecutableFunctionInfo<FunctionId: Copy> {
    fn function_num_regs(&self, function_id: FunctionId) -> Option<usize>;
}

#[derive(Debug, thiserror::Error)]
#[error("function {function_id:?} is not found in the functions table")]
pub struct FunctionNotFoundError {
    function_id: waymark_vm_bytecode::FunctionId,
}

impl<Interpreter, Executable, Value> Runtime<Interpreter, Executable, Value>
where
    Interpreter: waymark_vm_interpreter::Interpreter,
    Executable: ExecutableFunctionInfo<waymark_vm_bytecode::FunctionId>,
{
    pub fn with_conventional_entrypoint(
        interpreter: Interpreter,
        executable: Executable,
    ) -> Result<Self, FunctionNotFoundError> {
        Self::with_custom_entrypoint(
            interpreter,
            executable,
            CallSpec {
                func: IndexType::ZERO,
                args: Vec::new(),
            },
        )
    }

    pub fn with_custom_entrypoint(
        interpreter: Interpreter,
        executable: Executable,
        call: CallSpec<Value>,
    ) -> Result<Self, FunctionNotFoundError> {
        let mut ready = VecDeque::new();

        let CallSpec { func, args } = call;

        let num_regs = executable
            .function_num_regs(func)
            .ok_or(FunctionNotFoundError { function_id: func })?;

        let regs = Registers::new_for_fn_call(num_regs, args.into_iter().map(Promise::Resolved));

        ready.push_back(Frame {
            func,
            state: IndexType::ZERO,
            regs,
            kind: FrameKind::TopLevel,
        });

        let state = RuntimeState {
            ready,
            promise_states: PromiseStates::new(),
        };

        Ok(Self {
            interpreter,
            executable,
            state,
        })
    }
}

// pub enum Event<Spec: self::Spec> {
//     /// Extcall invocation is requested.
//     ExtCall {
//         /// The ID of the promise to resolve with the resulting value when
//         /// the extcall completes.
//         promise_id: PromiseId,

//         /// The ID of the extcall to invoke from the extcall table.
//         extcall_id: Spec::ExtCallId,

//         /// Args to pass to the extcall.
//         args: Vec<Spec::Value>,
//     },

//     /// Runtime suspension is requested.
//     Suspend {
//         /// The ID of the promise that must resolve before we can resume
//         /// the execution.
//         waiting_on: PromiseId,
//     },

//     /// Program execution is complete.
//     Complete(Spec::Value),
// }

#[derive(Debug)]
pub enum RunError<ExecutionError> {
    NoReadyFrame,
    Step(step::StepError<ExecutionError>),
}

impl<Interpreter, Executable, Value> Runtime<Interpreter, Executable, Value>
where
    Interpreter: waymark_vm_interpreter::Interpreter<Frame = Frame<Promise<Value>>>,
    for<'r> Interpreter::RuntimeView<'r>: From<(&'r Executable, &'r mut RuntimeState<Value>)>,
    Executable: step::InstructionsProvider<Instruction = Interpreter::Instruction>,
    Value: Clone,
{
    /// Run the VM steps until the next event is encountered.
    pub fn run(&mut self) -> Result<Interpreter::Effect, RunError<Interpreter::Error>> {
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

            return Ok(effect);
        }
    }

    /// Provide an async computation value for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn resolve_promise(
        &mut self,
        promise_id: PromiseStateId,
        val: Promise<Value>,
    ) -> Result<(), ResolvingAlreadyResolvedPromiseError> {
        self.state.resolve_promise(promise_id, val)
    }
}
