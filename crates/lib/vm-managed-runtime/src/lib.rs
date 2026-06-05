//! Managed runtime wrappers with typestate API.
//!
//! Separates a [`Runtime`] into two states:
//! - [`ActiveRuntime`]: has ready frames, can [`run`](ActiveRuntime::run).
//! - [`SuspendedRuntime`]: no ready frames, waiting on promise resolutions.
//!
//! [`ManagedRuntime`] is an enum over both, providing a single type that
//! callers can store and match on.

mod convert;
mod promises;
mod run;

#[cfg(feature = "snapshot")]
mod snapshot;

use waymark_vm_runtime::Runtime;

/// An error bundled with the [`Runtime`] that produced it.
///
/// Used in fallible operations that consume the runtime wrapper
/// so the caller can inspect or retry after a failure.
pub struct RuntimeError<Error, Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// The error that occurred.
    pub error: Error,

    /// The runtime at the time of the error.
    pub runtime: Runtime<Executable, Interpreter, Value>,
}

/// A runtime that has ready frames and can be stepped.
#[allow(dead_code)]
pub struct ActiveRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    runtime: Runtime<Executable, Interpreter, Value>,
}

/// A runtime that has no ready frames and is waiting on promises.
#[allow(dead_code)]
pub struct SuspendedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    runtime: Runtime<Executable, Interpreter, Value>,
}

/// A managed runtime in either active or suspended state.
#[allow(dead_code)]
pub enum ManagedRuntime<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// Runtime is actively executing.
    Active(ActiveRuntime<Executable, Interpreter, Value>),

    /// Runtime is suspended, waiting on promise resolutions.
    Suspended(SuspendedRuntime<Executable, Interpreter, Value>),
}

/// The result of [`ActiveRuntime::run`].
#[allow(dead_code)]
pub enum RunOutcome<Effect, Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
{
    /// An effect was emitted. The next state may be active or suspended.
    Effect {
        /// The emitted effect.
        effect: Effect,

        /// The runtime after producing the effect.
        runtime: ManagedRuntime<Executable, Interpreter, Value>,
    },

    /// No ready frames remain; the runtime is now suspended.
    Suspended {
        /// The suspended runtime.
        runtime: SuspendedRuntime<Executable, Interpreter, Value>,
    },
}
