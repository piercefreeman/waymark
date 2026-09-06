//! What a kind is to the readers: text, both ways.

/// A kind with a tag: the stable text the store column and the wire
/// carry.
pub trait Tagged {
    /// This kind's tag.
    fn tag(self) -> &'static str;
}

/// The kind some text names, if any.
///
/// The way back from text that came from outside — a filter, a stored
/// column — which may name no kind at all.
pub trait FromTag: Sized {
    /// The kind `tag` names.
    fn from_tag(tag: &str) -> Option<Self>;
}
