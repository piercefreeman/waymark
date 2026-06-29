//! Core conversion traits.

#![warn(missing_docs)]

/// Fallible conversion from one type to another.
pub trait TryConvert<From, To> {
    /// The error type returned when conversion fails.
    type Error;

    /// Convert `from` into `To`, or return an error.
    fn try_convert(from: From) -> Result<To, Self::Error>;
}

/// Conversion from one type to another.
pub trait Convert<From, To>: TryConvert<From, To, Error = core::convert::Infallible> {
    /// Convert `from` into `To`.
    fn convert(from: From) -> To;
}

impl<Converter, From, To> Convert<From, To> for Converter
where
    Converter: TryConvert<From, To, Error = core::convert::Infallible>,
{
    fn convert(from: From) -> To {
        let Ok(to) = Converter::try_convert(from);
        to
    }
}

/// The error type produced by a converter for a given source and target type.
pub type ConvertErrorFor<Converter, From, To> = <Converter as TryConvert<From, To>>::Error;

/// A converter based on [`core::convert::TryFrom`].
pub struct StdConverter;

impl<From, To> TryConvert<From, To> for StdConverter
where
    To: core::convert::TryFrom<From>,
{
    type Error = <To as core::convert::TryFrom<From>>::Error;

    fn try_convert(from: From) -> Result<To, Self::Error> {
        from.try_into()
    }
}
