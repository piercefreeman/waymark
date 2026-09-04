//! The classification every payload type carries.

/// Implemented by every payload type: what the readers may know about an
/// event without knowing its type.
///
/// The kind is the payload's own closed set — the readers match
/// on it, the API document lists its values, and a producer cannot
/// invent one the readers don't know. Its string form is the stable tag
/// the store and the wire carry, which is why a store bounds
/// `Self::Kind: Into<&'static str>` rather than naming the set.
pub trait EventKind {
    /// The closed set of kinds.
    type Kind;

    /// The event's kind.
    fn kind(&self) -> Self::Kind;
}
