//! The production payload.

use crate::Kind;

/// Every source's typed event, one variant per source: what the
/// production pipeline carries.
///
/// Empty until the first source lands; each source slice adds its
/// variant. The store serializes it as the event's `payload` at flush
/// and reads it back on query.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Payload {}

impl waymark_observability_events_core::EventKind for Payload {
    type Kind = Kind;

    fn kind(&self) -> Kind {
        match *self {}
    }
}
