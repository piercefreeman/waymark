//! Sound type-wrappers that restrict [`Arc`] interfaces to capture our required
//! semantics in a type-safe manner.

#![warn(clippy::missing_docs_in_private_items)]

use std::{ops::Deref, sync::Arc};

/// An [`Arc`] wrapper that doesn't permit direct cloning of
/// the wrapped [`Arc`], and instead only allows creation of
/// the [`RestrictedArc`]s.
///
/// Provides access to the underlying `T` just like the regular [`Arc`], and
/// a limited subset of other [`Arc`] APIs.
pub struct ControlledArc<T>(Arc<T>);

impl<T> ControlledArc<T> {
    /// Create a new [`ControlledArc`] from an owned `value`.
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }

    /// Create a new [`RestrictedArc`] by cloning the underlying [`Arc`].
    ///
    /// [`RestrictedArc`] in turn won't allow for creating more
    /// [`RestrictedArc`]s, so we know this is the only way the underlying
    /// [`Arc`]s strong count can be bumped.
    pub fn restricted_clone(value: &Self) -> RestrictedArc<T> {
        RestrictedArc(Arc::clone(&value.0))
    }

    /// See [`Arc::into_inner`].
    pub fn into_inner(value: Self) -> Option<T> {
        Arc::into_inner(value.0)
    }
}

impl<T> Deref for ControlledArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An [`Arc`] wrapper that doesn't permit cloning the wrapped [`Arc`], thus
/// preventing us from making new [`Arc`]s and bumping the strong count.
///
/// Provides access to the underlying `T` just like the regular [`Arc`], and
/// a limited subset of other [`Arc`] APIs.
pub struct RestrictedArc<T>(Arc<T>);

impl<T> RestrictedArc<T> {
    /// Convert this [`RestrictedArc`] into a [`StrongCountAccess`].
    ///
    /// This downgrades and drops the underlying [`Arc`], and passes
    /// the [`std::sync::Weak`] into the newly created [`StrongCountAccess`].
    ///
    /// This call effectively decrements the underlying [`Arc`]'s strong count
    /// by the virtue of getting rid on an owned [`Arc`] internal to
    /// the provided [`RestrictedArc`] `value`.
    pub fn into_strong_count_access(value: Self) -> StrongCountAccess<T> {
        let weak = Arc::downgrade(&value.0);
        drop(value);
        StrongCountAccess(weak)
    }
}

impl<T> Deref for RestrictedArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A wrapper around the [`std::sync::Weak`] that doesn't allow
/// upgrades but permits accessing [`std::sync::Weak::strong_count`].
pub struct StrongCountAccess<T>(std::sync::Weak<T>);

impl<T> StrongCountAccess<T> {
    /// Access [`std::sync::Weak::strong_count`].
    pub fn strong_count(&self) -> usize {
        self.0.strong_count()
    }
}
