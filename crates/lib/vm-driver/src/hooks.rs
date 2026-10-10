//! The driver loop's hooks, as one bound.

/// Every hook the driver loop calls, as one bound.
///
/// Implemented for every type that implements the five hooks traits.
pub trait Hooks:
    waymark_vm_driver_hooks::VmStarted
    + waymark_vm_driver_hooks::EffectEmitted
    + waymark_vm_driver_hooks::PromiseSettled
    + waymark_vm_driver_hooks::SnapshotPersisted
    + waymark_vm_driver_hooks::VmStopped
{
}

impl<T> Hooks for T where
    T: waymark_vm_driver_hooks::VmStarted
        + waymark_vm_driver_hooks::EffectEmitted
        + waymark_vm_driver_hooks::PromiseSettled
        + waymark_vm_driver_hooks::SnapshotPersisted
        + waymark_vm_driver_hooks::VmStopped
{
}

/// [`Hooks`] for the driver loop over the given higher-level types, the
/// ones [`run`](crate::run) takes: they observe the interpreter's effects,
/// the value's ready values, the raised exceptions promises are rejected
/// with, and the driver's terminal error.
///
/// Implemented for every [`Hooks`] whose types match.
pub trait HooksFor<Interpreter, Value, RaisedException, Effector, Persister, Codec>:
    Hooks
    + waymark_vm_driver_hooks::effect_emitted::HasEffect<Effect = Interpreter::Effect>
    + waymark_vm_driver_hooks::promise_settled::HasValue<Value = Value::ReadyValue>
    + waymark_vm_driver_hooks::promise_settled::HasRaisedException<RaisedException = RaisedException>
    + waymark_vm_driver_hooks::vm_stopped::HasError<
        Error = crate::ErrorFor<Interpreter, Codec, Persister, Effector>,
    >
where
    Interpreter: waymark_vm_interpreter::Interpreter,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Effector: waymark_vm_driver_core::EffectHandler + waymark_vm_driver_core::PromiseSettler,
    Persister: waymark_vm_driver_core::SnapshotPersister,
    Codec: waymark_vm_codec_core::SerializerProvider,
{
}

impl<T, Interpreter, Value, RaisedException, Effector, Persister, Codec>
    HooksFor<Interpreter, Value, RaisedException, Effector, Persister, Codec> for T
where
    T: Hooks
        + waymark_vm_driver_hooks::effect_emitted::HasEffect<Effect = Interpreter::Effect>
        + waymark_vm_driver_hooks::promise_settled::HasValue<Value = Value::ReadyValue>
        + waymark_vm_driver_hooks::promise_settled::HasRaisedException<
            RaisedException = RaisedException,
        > + waymark_vm_driver_hooks::vm_stopped::HasError<
            Error = crate::ErrorFor<Interpreter, Codec, Persister, Effector>,
        >,
    Interpreter: waymark_vm_interpreter::Interpreter,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Effector: waymark_vm_driver_core::EffectHandler + waymark_vm_driver_core::PromiseSettler,
    Persister: waymark_vm_driver_core::SnapshotPersister,
    Codec: waymark_vm_codec_core::SerializerProvider,
{
}
