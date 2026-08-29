//! Conditionally-required serde traits.
//!
//! A bound like `T: waymark_maybe_serde::Serialize` means "`T` must be
//! [`serde::Serialize`], but only when the `serde` feature is enabled;
//! no requirement otherwise". This lets generic code state serde
//! requirements in positions that cannot be `cfg`-gated — e.g. trait
//! associated-type bounds — while keeping serde support optional.
//!
//! Every trait is blanket-implemented, so these traits are never
//! implemented manually: they are satisfied automatically by every type
//! that meets the underlying serde requirement (or by every type at
//! all, with the feature disabled).

#![warn(missing_docs)]

/// [`serde::Serialize`] when the `serde` feature is enabled; no
/// requirement otherwise.
#[cfg(feature = "serde")]
pub trait Serialize: serde::Serialize {}

#[cfg(feature = "serde")]
impl<T: ?Sized + serde::Serialize> Serialize for T {}

/// [`serde::Serialize`] when the `serde` feature is enabled; no
/// requirement otherwise.
#[cfg(not(feature = "serde"))]
pub trait Serialize {}

#[cfg(not(feature = "serde"))]
impl<T: ?Sized> Serialize for T {}

/// [`serde::Deserialize`] when the `serde` feature is enabled; no
/// requirement otherwise.
#[cfg(feature = "serde")]
pub trait Deserialize<'de>: serde::Deserialize<'de> {}

#[cfg(feature = "serde")]
impl<'de, T: serde::Deserialize<'de>> Deserialize<'de> for T {}

/// [`serde::Deserialize`] when the `serde` feature is enabled; no
/// requirement otherwise.
#[cfg(not(feature = "serde"))]
pub trait Deserialize<'de> {}

#[cfg(not(feature = "serde"))]
impl<'de, T> Deserialize<'de> for T {}

/// [`serde::de::DeserializeOwned`] when the `serde` feature is
/// enabled; no requirement otherwise.
#[cfg(feature = "serde")]
pub trait DeserializeOwned: serde::de::DeserializeOwned {}

#[cfg(feature = "serde")]
impl<T: serde::de::DeserializeOwned> DeserializeOwned for T {}

/// [`serde::de::DeserializeOwned`] when the `serde` feature is
/// enabled; no requirement otherwise.
#[cfg(not(feature = "serde"))]
pub trait DeserializeOwned {}

#[cfg(not(feature = "serde"))]
impl<T> DeserializeOwned for T {}
