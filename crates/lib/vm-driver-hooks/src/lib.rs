//! Optional observers for the VM driver loop.
//!
//! The driver's required collaborators (the effector, the snapshot persister)
//! live in `waymark-vm-driver-core`; the hooks in this crate are the optional
//! ones. The driver calls a hook at each notable point of a run and carries
//! on: hooks are synchronous, infallible, and never on the error path.
//!
//! A hooks value is specific to one driver run, exactly as the effector is.
//! The driver knows no identity, so a hook that needs to know which VM it
//! observes is constructed with that knowledge by whoever starts the run.

#![warn(missing_docs)]

pub mod effect_emitted;
pub mod promise_settled;
pub mod snapshot_persisted;
pub mod vm_started;
pub mod vm_stopped;

pub use self::effect_emitted::EffectEmitted;
pub use self::promise_settled::PromiseSettled;
pub use self::snapshot_persisted::SnapshotPersisted;
pub use self::vm_started::VmStarted;
pub use self::vm_stopped::VmStopped;
