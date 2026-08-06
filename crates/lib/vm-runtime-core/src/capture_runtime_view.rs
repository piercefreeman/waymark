use crate::RuntimeState;

/// The full view into the runtime.
pub struct FullRuntimeView<'r, Executable, FunctionId, StateId, Value> {
    /// A ref access to the executable we're executing.
    pub executable: &'r Executable,

    /// A mut access to the runtime state.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value>,
}
