//! Implementations of [`waymark_action_runtime_core::ActionCallRequester`]
//! and [`waymark_action_runtime_core::ActionCallCompletionsProvider`] backed by
//! a [`waymark_worker_core::BaseWorkerPool`].

#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use uuid::Uuid;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

mod completions_provider;
mod requester;

pub use self::completions_provider::direct::*;
pub use self::completions_provider::routed::*;
pub use self::requester::*;

/// Shared correlation map from dispatch tokens to their effect number and
/// promise state ID.  The requester inserts entries when dispatching actions,
/// and the completions provider looks them up when results arrive.
pub type DispatchCorrelationMap = Arc<Mutex<HashMap<Uuid, (EffectNumber, PromiseStateId)>>>;
