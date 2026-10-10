//! Serialization and deserialization of the VM runtime state.

use crate::Runtime;

use waymark_vm_runtime_core::RuntimeState;

impl<Executable, Interpreter, Value, RaisedException>
    Runtime<Executable, Interpreter, Value, RaisedException>
where
    Executable: waymark_vm_executable::FunctionStates,
    Executable::FunctionId: serde::Serialize,
    Executable::StateId: serde::Serialize,
    Value: serde::Serialize,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
    RaisedException: serde::Serialize,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
{
    /// Serialize the runtime state using the provided [`Serializer`](serde::Serializer).
    pub fn snapshot<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.state, serializer)
    }
}

impl<Executable, Interpreter, Value, RaisedException>
    Runtime<Executable, Interpreter, Value, RaisedException>
where
    Executable: waymark_vm_executable::FunctionStates,
    Executable::FunctionId: for<'de> serde::Deserialize<'de>,
    Executable::StateId: for<'de> serde::Deserialize<'de>,
    Value: for<'de> serde::Deserialize<'de>,
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
    RaisedException: for<'de> serde::Deserialize<'de>,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: for<'de> serde::Deserialize<'de>,
{
    /// Restore a runtime from a snapshot [`Deserializer`](serde::Deserializer).
    ///
    /// Takes the same interpreter and executable that were used to create
    /// the original runtime, along with a deserializer positioned at the
    /// previously snapshotted state.
    pub fn from_snapshot<'de, D: serde::Deserializer<'de>>(
        interpreter: Interpreter,
        executable: Executable,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let state: RuntimeState<
            Executable::FunctionId,
            Executable::StateId,
            Value,
            RaisedException,
        > = serde::Deserialize::deserialize(deserializer)?;

        Ok(Self {
            interpreter,
            executable,
            state,
        })
    }
}
