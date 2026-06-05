use waymark_vm_runtime::Runtime;

use crate::{ActiveRuntime, ManagedRuntime, SuspendedRuntime};

impl<Executable, Interpreter, Value> ActiveRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// Try to create an active runtime from a raw [`Runtime`].
    ///
    /// Returns `Err(runtime)` if the runtime has no ready frames.
    pub fn new(
        runtime: Runtime<Executable, Interpreter, Value>,
    ) -> Result<Self, Runtime<Executable, Interpreter, Value>> {
        if runtime.has_ready_frames() {
            Ok(Self { runtime })
        } else {
            Err(runtime)
        }
    }
}

impl<Executable, Interpreter, Value> SuspendedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// Try to create a suspended runtime from a raw [`Runtime`].
    ///
    /// Returns `Err(runtime)` if the runtime has ready frames.
    pub fn new(
        runtime: Runtime<Executable, Interpreter, Value>,
    ) -> Result<Self, Runtime<Executable, Interpreter, Value>> {
        if runtime.has_ready_frames() {
            Err(runtime)
        } else {
            Ok(Self { runtime })
        }
    }
}

impl<Executable, Interpreter, Value> ManagedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// Wrap a raw [`Runtime`] into the appropriate variant.
    pub fn from_runtime(runtime: Runtime<Executable, Interpreter, Value>) -> Self {
        if runtime.has_ready_frames() {
            Self::Active(ActiveRuntime { runtime })
        } else {
            Self::Suspended(SuspendedRuntime { runtime })
        }
    }

    /// Unwrap the managed runtime into the inner [`Runtime`].
    pub fn into_runtime(self) -> Runtime<Executable, Interpreter, Value> {
        match self {
            Self::Active(active) => active.runtime,
            Self::Suspended(suspended) => suspended.runtime,
        }
    }
}

impl<Executable, Interpreter, Value> From<ActiveRuntime<Executable, Interpreter, Value>>
    for ManagedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    fn from(active: ActiveRuntime<Executable, Interpreter, Value>) -> Self {
        Self::Active(active)
    }
}

impl<Executable, Interpreter, Value> From<SuspendedRuntime<Executable, Interpreter, Value>>
    for ManagedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    fn from(suspended: SuspendedRuntime<Executable, Interpreter, Value>) -> Self {
        Self::Suspended(suspended)
    }
}

impl<Executable, Interpreter, Value> From<Runtime<Executable, Interpreter, Value>>
    for ManagedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    fn from(runtime: Runtime<Executable, Interpreter, Value>) -> Self {
        Self::from_runtime(runtime)
    }
}

impl<Executable, Interpreter, Value> From<ManagedRuntime<Executable, Interpreter, Value>>
    for Runtime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    fn from(runtime: ManagedRuntime<Executable, Interpreter, Value>) -> Self {
        runtime.into_runtime()
    }
}
