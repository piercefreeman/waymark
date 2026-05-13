//! Empty-or-non-empty vector helper.

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};

/// A vector-like container that is either empty or provably non-empty.
///
/// "EE" stands for "explicitly-empty", akin to non-empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EEVec<T> {
    /// No items are present.
    Empty,

    /// One or more items are present.
    NonEmpty(NEVec<T>),
}

impl<T> EEVec<T> {
    /// Returns an iterator over the contained items.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        match self {
            Self::Empty => [].iter(),
            Self::NonEmpty(nevec) => nevec.iter(),
        }
    }

    /// Maps each item while preserving the empty-or-non-empty invariant.
    pub fn try_map<U, E, F>(self, map_item: F) -> Result<EEVec<U>, E>
    where
        F: FnMut(T) -> Result<U, E>,
    {
        match self {
            Self::Empty => Ok(EEVec::Empty),
            Self::NonEmpty(items) => {
                let items: NEVec<_> = items
                    .into_nonempty_iter()
                    .map(map_item)
                    .collect::<Result<_, _>>()?;
                Ok(EEVec::NonEmpty(items))
            }
        }
    }

    /// Borrows the contents as a slice.
    pub fn as_slice(&self) -> &[T] {
        match self {
            Self::Empty => &[],
            Self::NonEmpty(nevec) => nevec.as_ref(),
        }
    }
}

impl<T> AsRef<[T]> for EEVec<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> IntoIterator for EEVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Empty => Vec::new().into_iter(),
            Self::NonEmpty(nevec) => nevec.into_iter(),
        }
    }
}
