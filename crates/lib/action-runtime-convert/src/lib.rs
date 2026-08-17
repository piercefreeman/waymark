//! A converter that provides conversion for the action runtime.

#![warn(missing_docs)]

mod from_proto;
mod to_dispatch;
mod to_proto;

/// A converter that provides conversion for the action runtime.
pub struct Converter;

/// The result named no outcome, so the worker never said how the call
/// completed — neither a returned value nor a raised exception, which is
/// a worker that violated the protocol rather than a call that produced
/// nothing.
#[derive(Debug, thiserror::Error)]
#[error("the action result names no outcome")]
pub struct MissingOutcomeError;

/// Error reading the result an [`ActionResult`] carries.
///
/// [`ActionResult`]: waymark_proto::messages::ActionResult
#[derive(Debug, thiserror::Error)]
pub enum ActionResultError {
    /// The encoded result could not be decoded.
    #[error("decoding the action result")]
    Decode(#[source] prost::DecodeError),

    /// The decoded result did not say how the call completed.
    #[error("reading the action result's outcome")]
    Outcome(#[source] MissingOutcomeError),
}
