//! The closed classification of events.

use std::collections::HashMap;
use std::sync::LazyLock;

use waymark_observability_events_core::kind::Kind as _;

/// Which event, across every source.
///
/// Closed: the readers match on it, the API document lists its values,
/// and a producer cannot invent one the readers don't know. One variant
/// per source, carrying that source's own kinds.
///
/// Its tag is the stable text — the store's `kind` column and the wire
/// value. Each source spells out its own tags, the path source first;
/// [`Tagged`] delegates to the source, and [`FromTag`] looks the text up,
/// never parses it, in a table built from the whole family
/// ([`SubsetExt`]) — so the two directions cannot disagree.
///
/// [`Tagged`]: waymark_observability_events_core::kind::Tagged
/// [`FromTag`]: waymark_observability_events_core::kind::FromTag
/// [`SubsetExt`]: waymark_observability_events_core::kind::SubsetExt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {}

// The root of the family: its own subset, the whole of it.
impl waymark_observability_events_core::kind::Subset for Kind {
    type RootKind = Kind;

    fn subset() -> impl Iterator<Item = Self> {
        std::iter::empty()
    }

    fn root_kind(&self) -> Kind {
        *self
    }

    fn from_root_kind(root: Kind) -> Option<Self> {
        Some(root)
    }
}

impl waymark_observability_events_core::kind::Tagged for Kind {
    fn tag(&self) -> &'static str {
        match *self {}
    }
}

impl waymark_observability_events_core::kind::FromTag for Kind {
    fn from_tag(tag: &str) -> Option<Kind> {
        /// Tag → kind, built once from the whole family.
        static BY_TAG: LazyLock<HashMap<&'static str, Kind>> = LazyLock::new(|| {
            Kind::all()
                .map(|kind| {
                    (
                        waymark_observability_events_core::kind::Tagged::tag(&kind),
                        kind,
                    )
                })
                .collect()
        });

        BY_TAG.get(tag).copied()
    }
}
