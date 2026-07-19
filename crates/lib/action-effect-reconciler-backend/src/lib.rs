//! Backend traits for durably-stored action-call requests.
//!
//! Defines the contract that a database backend must fulfill to support
//! durable handling of action-call effects: a request row is recorded
//! (born locked) when the VM emits the effect, locked/relocked for
//! delivery at VM revival reconcile, kept alive by lock renewal while the
//! attempt runs in the owner's local worker pool, and unlocked at graceful
//! shutdown if its call was never delivered.
//!
//! # The removal invariant
//!
//! There is deliberately no removal operation here: **a request row is
//! removed atomically by the store itself the moment its completion is
//! durably recorded** (in postgres, a trigger on the completions table).
//! Implementations must uphold this invariant.  Consequently, the
//! existence of a request row *means* its outcome has not been durably
//! recorded yet.  Rows of a VM that no longer exists are likewise
//! removed by the store itself alongside the VM (in postgres, a trigger
//! on snapshot deletion).
//!
//! # Composition constraint
//!
//! These traits and the durable-completions backend traits must be
//! implemented over ONE atomic store — the removal invariant is
//! unenforceable across separate stores, so implementing the two trait
//! families over different databases is an invalid composition.

#![warn(missing_docs)]

mod common;

pub mod lock_vm_action_call_requests;
pub mod record_action_call_requests;
pub mod renew_action_call_request_locks;
pub mod unlock_action_call_requests;

pub use self::common::*;
pub use self::lock_vm_action_call_requests::LockVmActionCallRequests;
pub use self::record_action_call_requests::RecordActionCallRequests;
pub use self::renew_action_call_request_locks::RenewActionCallRequestLocks;
pub use self::unlock_action_call_requests::UnlockActionCallRequests;
