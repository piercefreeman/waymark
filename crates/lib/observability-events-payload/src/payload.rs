//! The production payload.

use crate::Kind;

/// Every source's typed event, one variant per source: what the
/// production pipeline carries.
///
/// The store serializes it as the event's `payload` at flush and reads
/// it back on query. Tagged internally: the source is the `source` field
/// and the source's own fields sit beside it, so a reader addresses them
/// directly (`payload ->> 'vm_id'`).
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum Payload {
    /// An event of the VM driver.
    VmDriver(crate::vm_driver::Payload),
}

impl waymark_observability_events_core::Kinded for Payload {
    type Kind = Kind;

    fn kind(&self) -> Kind {
        match self {
            Payload::VmDriver(payload) => Kind::VmDriver(payload.kind()),
        }
    }
}
