use crate::RuntimeState;

/// An ability to capture a view to a runtime.
///
/// To be used by the instruction set interpreters when they need a view to
/// a runtime.
///
/// This indirection is provided to reduce the coupling between the runtime
/// and the interpreters, and make the dependency of the interpreter on
/// the runtime state more explicit.
pub trait CaptureRuntimeView<Executable, FunctionId, StateId, Value> {
    /// The view into the runtime to capture.
    type RuntimeView<'v>
    where
        Executable: 'v,
        FunctionId: 'v,
        StateId: 'v,
        Value: 'v;

    /// Capture the recuded runtime view from the full runtime view.
    fn capture_runtime_view<'r>(
        view: FullRuntimeView<'r, Executable, FunctionId, StateId, Value>,
    ) -> Self::RuntimeView<'r>;
}

/// The full view into the runtime.
pub struct FullRuntimeView<'r, Executable, FunctionId, StateId, Value> {
    /// A ref access to the executable we're executing.
    pub executable: &'r Executable,

    /// A mut access to the runtime state.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value>,
}
