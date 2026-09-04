//! The closed classification of events.

use std::collections::HashMap;
use std::sync::LazyLock;

/// Which event, across every source.
///
/// Closed: the readers match on it, the API document lists its values,
/// and a producer cannot invent one the readers don't know. Empty until
/// the first source lands; each source slice adds its variants.
///
/// Its tag is the stable text — the store's `kind` column and the wire
/// value — spelled out per variant in [`Tagged`], and looked up, never
/// parsed, in [`FromTag`]: the table there is built from [`Kind::all`],
/// so the two directions cannot disagree.
///
/// [`Tagged`]: waymark_observability_events_core::kind::Tagged
/// [`FromTag`]: waymark_observability_events_core::kind::FromTag
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {}

impl Kind {
    /// Every kind, across every source, once.
    pub fn all() -> impl Iterator<Item = Kind> {
        std::iter::empty()
    }
}

/// Tag → kind, built once from [`Kind::all`].
static BY_TAG: LazyLock<HashMap<&'static str, Kind>> = LazyLock::new(|| {
    Kind::all()
        .map(|kind| {
            (
                waymark_observability_events_core::kind::Tagged::tag(kind),
                kind,
            )
        })
        .collect()
});

impl waymark_observability_events_core::kind::Tagged for Kind {
    fn tag(self) -> &'static str {
        match self {}
    }
}

impl waymark_observability_events_core::kind::FromTag for Kind {
    fn from_tag(tag: &str) -> Option<Kind> {
        BY_TAG.get(tag).copied()
    }
}
