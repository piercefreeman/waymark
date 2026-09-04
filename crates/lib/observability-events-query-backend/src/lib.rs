//! Backend traits for querying the observability-events store.
//!
//! Each read has its own order, and a position in that order — the
//! cursor — is the backend's own type: it names a row in whatever shape
//! the backend pages by, which is the backend's private business.

#![warn(missing_docs)]

mod common;
pub mod list_events;
pub mod tail;

pub use self::common::*;

pub use self::list_events::ListEvents;
pub use self::tail::Tail;
