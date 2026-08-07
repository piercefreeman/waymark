//! A phantom-generic uninhabited type.
//!
//! An uninhabited marker type without type parameters is best expressed
//! as an empty enum — self-evidently uninhabited, no machinery needed.
//! With a type parameter that stops working: an empty enum cannot use
//! its parameter (E0392). [`PhantomUninhabitable`] is the pattern for
//! that case — it pairs an [`Infallible`](core::convert::Infallible)
//! (making it uninhabited) with a [`PhantomData`](core::marker::PhantomData)
//! (using the parameter). Newtype it to define a generic uninhabited
//! type.

#![warn(missing_docs)]

/// An uninhabited type generic over `T`.
///
/// No value of this type can ever exist; it is useful purely at the type
/// level. Completely opaque — newtype it to define your own generic
/// uninhabited types (wrapping only requires naming the type, never a
/// value of it).
pub struct PhantomUninhabitable<T>(core::convert::Infallible, core::marker::PhantomData<T>);

impl<T> core::fmt::Debug for PhantomUninhabitable<T> {
    fn fmt(&self, _formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {}
    }
}
