//! The operations machinery for the VM interpreters.
//!
//! Decouples the value *shape* from the *operations* over values: the
//! set-interpreters' vocabulary traits are implemented on operations
//! types rather than on the value types themselves, and the interpreters
//! are generic over the operations. [`Operations`] is the provided
//! wrapper those implementations attach to; the variation marker selects
//! which set of implementations — a language, primarily — the assembled
//! stack runs with.

#![warn(missing_docs)]

pub mod coreset;
pub mod extcallset;
pub mod promise;
pub mod pureset;

/// The VM interpreter operations, specialized by the variation marker.
pub struct Operations<Variation>(waymark_phantom_uninhabitable::PhantomUninhabitable<Variation>);
