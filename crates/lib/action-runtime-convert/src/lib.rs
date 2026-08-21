//! A converter that provides conversion for the action runtime.
//!
//! [`Converter`] is generic over a wrapped value converter: the envelope
//! logic — dispatch assembly and loss lowering — is written once here,
//! and everything value-specific is delegated through
//! [`TryConvert`](waymark_convert_core::TryConvert) bounds on the
//! `ValueConverter` parameter.  Wiring pins the flavor's own conversion
//! crate as the parameter; nothing in this crate names a flavor.

#![warn(missing_docs)]

mod loss;
mod to_dispatch;

/// A converter that provides conversion for the action runtime.
///
/// Generic over the `ValueConverter` performing the value-level work;
/// use the parameterized type where envelope conversions are needed and
/// the bare value converter where value-level ones are.
pub struct Converter<ValueConverter> {
    _value_converter: core::marker::PhantomData<ValueConverter>,
}
