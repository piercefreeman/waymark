use waymark_vm_runtime_core::{RejectPromiseError, ResolvePromiseError};
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{ActiveRuntime, ManagedRuntime, RuntimeError, SuspendedRuntime};

impl<Executable, Interpreter, Value> ActiveRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Value: waymark_vm_runtime_promise_core::Resolvable + Clone,
{
    /// Resolve a pending promise with a ready value.
    ///
    /// Always returns an [`ActiveRuntime`] since resolving a promise
    /// cannot remove existing ready frames.
    #[allow(clippy::type_complexity)]
    pub fn resolve_promise(
        mut self,
        promise_state_id: PromiseStateId,
        value: Value::ReadyValue,
    ) -> Result<
        ActiveRuntime<Executable, Interpreter, Value>,
        RuntimeError<ResolvePromiseError<Value::ReadyValue>, Executable, Interpreter, Value>,
    > {
        debug_assert!(self.runtime.has_ready_frames());
        match self.runtime.resolve_promise(promise_state_id, value) {
            Ok(()) => {
                debug_assert!(self.runtime.has_ready_frames());
                Ok(ActiveRuntime {
                    runtime: self.runtime,
                })
            }
            Err(error) => Err(RuntimeError {
                error,
                runtime: self.runtime,
            }),
        }
    }

    /// Reject a pending promise with an exception.
    #[allow(clippy::type_complexity)]
    pub fn reject_promise(
        mut self,
        promise_state_id: PromiseStateId,
        exception: waymark_vm_runtime_exception::Exception<Value::ReadyValue>,
    ) -> Result<
        ActiveRuntime<Executable, Interpreter, Value>,
        RuntimeError<RejectPromiseError<Value::ReadyValue>, Executable, Interpreter, Value>,
    > {
        debug_assert!(self.runtime.has_ready_frames());
        match self.runtime.reject_promise(promise_state_id, exception) {
            Ok(()) => {
                debug_assert!(self.runtime.has_ready_frames());
                Ok(ActiveRuntime {
                    runtime: self.runtime,
                })
            }
            Err(error) => Err(RuntimeError {
                error,
                runtime: self.runtime,
            }),
        }
    }
}

impl<Executable, Interpreter, Value> SuspendedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Value: waymark_vm_runtime_promise_core::Resolvable + Clone,
{
    /// Resolve a pending promise with a ready value.
    ///
    /// Consumes `self` and returns the runtime in its new state.
    /// The returned runtime may be active if continuations were awakened.
    #[allow(clippy::type_complexity)]
    pub fn resolve_promise(
        mut self,
        promise_state_id: PromiseStateId,
        value: Value::ReadyValue,
    ) -> Result<
        ManagedRuntime<Executable, Interpreter, Value>,
        RuntimeError<ResolvePromiseError<Value::ReadyValue>, Executable, Interpreter, Value>,
    > {
        debug_assert!(!self.runtime.has_ready_frames());
        match self.runtime.resolve_promise(promise_state_id, value) {
            Ok(()) => Ok(ManagedRuntime::from_runtime(self.runtime)),
            Err(error) => Err(RuntimeError {
                error,
                runtime: self.runtime,
            }),
        }
    }

    /// Reject a pending promise with an exception.
    #[allow(clippy::type_complexity)]
    pub fn reject_promise(
        mut self,
        promise_state_id: PromiseStateId,
        exception: waymark_vm_runtime_exception::Exception<Value::ReadyValue>,
    ) -> Result<
        ManagedRuntime<Executable, Interpreter, Value>,
        RuntimeError<RejectPromiseError<Value::ReadyValue>, Executable, Interpreter, Value>,
    > {
        debug_assert!(!self.runtime.has_ready_frames());
        match self.runtime.reject_promise(promise_state_id, exception) {
            Ok(()) => Ok(ManagedRuntime::from_runtime(self.runtime)),
            Err(error) => Err(RuntimeError {
                error,
                runtime: self.runtime,
            }),
        }
    }
}
