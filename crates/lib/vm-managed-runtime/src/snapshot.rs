use crate::{ActiveRuntime, ManagedRuntime, SuspendedRuntime};

impl<Executable, Interpreter, Value> ActiveRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Executable::FunctionId: serde::Serialize,
    Executable::StateId: serde::Serialize,
    Value: serde::Serialize,
{
    /// Serialize the active runtime using the provided
    /// [`Serializer`](serde::Serializer).
    pub fn snapshot<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        debug_assert!(self.runtime.has_ready_frames());
        self.runtime.snapshot(serializer)
    }
}

impl<Executable, Interpreter, Value> SuspendedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Executable::FunctionId: serde::Serialize,
    Executable::StateId: serde::Serialize,
    Value: serde::Serialize,
{
    /// Serialize the suspended runtime using the provided
    /// [`Serializer`](serde::Serializer).
    pub fn snapshot<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        debug_assert!(!self.runtime.has_ready_frames());
        self.runtime.snapshot(serializer)
    }
}

impl<Executable, Interpreter, Value> ManagedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Executable::FunctionId: serde::Serialize,
    Executable::StateId: serde::Serialize,
    Value: serde::Serialize,
{
    /// Serialize the managed runtime using the provided
    /// [`Serializer`](serde::Serializer).
    pub fn snapshot<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Active(active) => active.snapshot(serializer),
            Self::Suspended(suspended) => suspended.snapshot(serializer),
        }
    }
}
