//! The page size of a read, capped at the read's own maximum.
//!
//! A read's backend states in its parameters how large a page it is
//! willing to serve, as the type: `Limit<1000>` is a page of one to a
//! thousand. The cap is checked once, on construction, so a backend
//! never sees a page size it did not agree to, and a transport turns the
//! construction error into its own rejection with nothing to know.

#![warn(missing_docs)]

use std::num::NonZeroUsize;

/// A page size within `1..=MAX`.
///
/// `MAX` must be at least 1 — a cap of zero admits no page size at all
/// and is refused at compile time:
///
/// ```compile_fail
/// let _ = waymark_query_limit::Limit::<0>::new(1);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Limit<const MAX: usize>(NonZeroUsize);

/// A page size outside `1..=MAX`.
#[derive(Debug, thiserror::Error)]
#[error("limit must be within 1..={MAX}, got {value}")]
pub struct LimitError<const MAX: usize> {
    /// The page size asked for.
    pub value: usize,
}

impl<const MAX: usize> Limit<MAX> {
    /// Evaluated by every construction, so a `Limit<0>` fails to compile
    /// wherever one would be made.
    const ASSERT_MAX_IS_POSITIVE: () = assert!(MAX >= 1, "a limit's cap must be at least 1");

    /// A page size of `value`, when `value` is within `1..=MAX`.
    pub const fn new(value: usize) -> Result<Self, LimitError<MAX>> {
        let () = Self::ASSERT_MAX_IS_POSITIVE;
        if value > MAX {
            return Err(LimitError { value });
        }
        match NonZeroUsize::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(LimitError { value }),
        }
    }

    /// The page size.
    pub const fn get(self) -> usize {
        self.0.get()
    }

    /// The page size, as the non-zero it is.
    pub const fn get_nonzero(self) -> NonZeroUsize {
        self.0
    }
}

#[cfg(test)]
mod tests;
