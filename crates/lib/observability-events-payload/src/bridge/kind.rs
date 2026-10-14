/// Which bridge event.
///
/// Mirrors [`Observation`](super::Observation) without its data. The tag
/// is the path, source first: `bridge.executable_registered`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// An executable was registered.
    ExecutableRegistered,
}

impl Kind {
    /// The kind's tag: the stable text the store and the wire carry.
    pub const fn tag(self) -> &'static str {
        match self {
            Kind::ExecutableRegistered => "bridge.executable_registered",
        }
    }
}

impl waymark_observability_events_core::kind::Subset for Kind {
    type RootKind = crate::Kind;

    fn subset() -> impl Iterator<Item = Self> {
        [Kind::ExecutableRegistered].into_iter()
    }

    fn root_kind(&self) -> crate::Kind {
        crate::Kind::Bridge(*self)
    }

    fn from_root_kind(root: crate::Kind) -> Option<Self> {
        match root {
            crate::Kind::Bridge(kind) => Some(kind),
            _ => None,
        }
    }
}
