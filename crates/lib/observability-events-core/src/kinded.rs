//! The classification every payload type carries.

/// Implemented by every payload type: a payload with a kind — what the
/// readers may know about an event without knowing its type.
///
/// The kind is the payload's own closed set — the readers match
/// on it, the API document lists its values, and a producer cannot
/// invent one the readers don't know. Its tag is the stable text the
/// store and the wire carry, both ways ([`kind::Tagged`] and
/// [`kind::FromTag`]) — bounded here, so no reader has to ask for it.
///
/// [`kind::Tagged`]: crate::kind::Tagged
/// [`kind::FromTag`]: crate::kind::FromTag
pub trait Kinded {
    /// The closed set of kinds.
    type Kind: crate::kind::Tagged + crate::kind::FromTag;

    /// The event's kind.
    fn kind(&self) -> Self::Kind;
}
