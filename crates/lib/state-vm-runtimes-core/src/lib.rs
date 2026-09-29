//! Core traits for the state-vm-runtimes subsystem.
//!
//! Provides the [`EffectorProvider`], [`InterpreterProvider`] and
//! [`HooksProvider`] traits — abstractions for creating per-VM effectors,
//! interpreters and driver hooks.

#![warn(missing_docs)]

/// Provides an effector for a given VM.
///
/// The effector is the composite handler that the VM driver uses to process
/// interpreter effects and settle promises. Since effectors are typically
/// bound to a specific VM identity and backend, this trait allows creating
/// them on demand when a VM is revived, rather than requiring a single
/// shared effector at factory construction time.
pub trait EffectorProvider {
    /// The VM identifier type.
    type VmId;

    /// The effector type produced by this provider.
    type Effector;

    /// Provide an effector for the given VM.
    fn provide_effector(&self, vm_id: &Self::VmId) -> Self::Effector;
}

/// Provides the driver hooks for a given VM.
///
/// The driver knows no identity; the hooks observing a run are bound to
/// the VM they observe here, when the VM is revived, exactly as the
/// effector is.
pub trait HooksProvider {
    /// The VM identifier type.
    type VmId;

    /// The hooks type produced by this provider.
    type Hooks;

    /// Provide the hooks for the given VM.
    fn provide_hooks(&self, vm_id: &Self::VmId) -> Self::Hooks;
}

/// Provides an interpreter for a given VM.
pub trait InterpreterProvider {
    /// The VM identifier type.
    type VmId;

    /// The interpreter type produced by this provider.
    type Interpreter;

    /// Provide an interpreter for the given VM.
    fn provide_interpreter(&self, vm_id: &Self::VmId) -> Self::Interpreter;
}

/// An [`InterpreterProvider`] that always returns a default-constructed
/// interpreter, ignoring the VM identifier.
///
/// Useful for stateless interpreters that implement [`Default`].
pub struct DefaultInterpreterProvider<Interpreter, VmId> {
    _phantom: core::marker::PhantomData<(Interpreter, VmId)>,
}

impl<Interpreter, VmId> DefaultInterpreterProvider<Interpreter, VmId> {
    /// Create a new default interpreter provider.
    pub fn new() -> Self {
        Self {
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<Interpreter, VmId> Default for DefaultInterpreterProvider<Interpreter, VmId> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Interpreter: Default, VmId> InterpreterProvider
    for DefaultInterpreterProvider<Interpreter, VmId>
{
    type VmId = VmId;
    type Interpreter = Interpreter;

    fn provide_interpreter(&self, _vm_id: &Self::VmId) -> Self::Interpreter {
        Interpreter::default()
    }
}

/// An [`EffectorProvider`] that wraps a closure.
pub struct FnEffectorProvider<F, VmId> {
    f: F,
    phantom_data: std::marker::PhantomData<fn(&VmId)>,
}

impl<F, VmId> FnEffectorProvider<F, VmId> {
    /// Create a new [`FnEffectorProvider`] from a given `f`.
    pub fn new(f: F) -> Self {
        Self {
            f,
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<F, VmId, Effector> EffectorProvider for FnEffectorProvider<F, VmId>
where
    F: Fn(&VmId) -> Effector,
{
    type VmId = VmId;
    type Effector = Effector;

    fn provide_effector(&self, vm_id: &Self::VmId) -> Self::Effector {
        (self.f)(vm_id)
    }
}

/// A [`HooksProvider`] that wraps a closure.
pub struct FnHooksProvider<F, VmId> {
    f: F,
    phantom_data: std::marker::PhantomData<fn(&VmId)>,
}

impl<F, VmId> FnHooksProvider<F, VmId> {
    /// Create a new [`FnHooksProvider`] from a given `f`.
    pub fn new(f: F) -> Self {
        Self {
            f,
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<F, VmId, Hooks> HooksProvider for FnHooksProvider<F, VmId>
where
    F: Fn(&VmId) -> Hooks,
{
    type VmId = VmId;
    type Hooks = Hooks;

    fn provide_hooks(&self, vm_id: &Self::VmId) -> Self::Hooks {
        (self.f)(vm_id)
    }
}
