//! Cursors: positions in a read's order, in the backend's own shape,
//! with an opaque wire form — written by a backend as a page's `next`,
//! handed back to it as the next read's `after`, and nothing else.
//!
//! A read trait bounds its position type on [`Cursor`], so no backend
//! can offer a read without a codec for its position; a transport
//! writes through [`EncodeCursor`] and reads through [`DecodeCursor`].

#![warn(missing_docs)]

/// Writes a position as its opaque wire form.
pub trait EncodeCursor {
    /// The wire form.
    fn encode(&self) -> String;
}

/// Reads a position back from its wire form.
pub trait DecodeCursor: Sized {
    /// Why a wire form could not be read back.
    type Error: std::fmt::Debug;

    /// A position from its wire form.
    fn decode(text: &str) -> Result<Self, Self::Error>;
}

/// A position in a read's order, in the backend's own shape, with an
/// opaque wire form: both halves of the codec, as one bound.
pub trait Cursor: EncodeCursor + DecodeCursor {}

impl<T> Cursor for T where T: EncodeCursor + DecodeCursor {}
