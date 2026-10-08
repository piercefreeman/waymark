use crate::RuntimeState;

/// The full view into the runtime.
pub struct FullRuntimeView<'r, Executable, FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// A ref access to the executable we're executing.
    pub executable: &'r Executable,

    /// A mut access to the runtime state.
    pub state: &'r mut RuntimeState<FunctionId, StateId, Value, RaisedException>,
}

impl<'s, 'r, Executable, FunctionId, StateId, Value, RaisedException>
    waymark_vm_runtime_view_capture::CaptureRuntimeView<
        's,
        FullRuntimeView<'r, Executable, FunctionId, StateId, Value, RaisedException>,
    > for FullRuntimeView<'s, Executable, FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    fn capture_runtime_view(
        source: &'s mut FullRuntimeView<
            'r,
            Executable,
            FunctionId,
            StateId,
            Value,
            RaisedException,
        >,
    ) -> Self {
        FullRuntimeView {
            executable: source.executable,
            state: &mut *source.state,
        }
    }
}
