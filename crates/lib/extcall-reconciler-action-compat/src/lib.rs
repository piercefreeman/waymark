//! Compatibility adapters implementing the extcall-reconciler-core action
//! traits over the action-runtime-core interfaces.
//!
//! [`effect_handler::EffectHandler`] implements
//! [`waymark_extcall_reconciler_core::ActionEffectHandler`] over any
//! [`waymark_action_runtime_core::ActionCallRequester`], and
//! [`promise_settler::PromiseSettler`] implements
//! [`waymark_extcall_reconciler_core::ActionPromiseSettler`] over any
//! [`waymark_action_runtime_core::ActionCallCompletionsProvider`].
//!
//! This is a deliberately thin, logic-less layer: the adapters carry no
//! buffering, no acknowledgement semantics (settlement acks are no-ops),
//! and no demand handling — they only translate between the two trait
//! vocabularies, which makes them suitable for transient execution paths.
//! Stateful settlement semantics (e.g. durable storage with real acks)
//! belong in dedicated implementations.
//!
//! Each adapter stands alone, so the dispatch and settlement halves can be
//! mixed freely with other implementations (e.g. a durable settler paired
//! with the requester-backed effect handler).

#![warn(missing_docs)]

pub mod effect_handler;
pub mod promise_settler;

pub use self::effect_handler::EffectHandler;
pub use self::promise_settler::PromiseSettler;
