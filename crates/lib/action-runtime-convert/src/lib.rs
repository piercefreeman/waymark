//! A converter that provides conversion for the action runtime.

#![warn(missing_docs)]

mod from_proto;
mod to_dispatch;
mod to_proto;

/// A converter that provides conversion for the action runtime.
pub struct Converter;

/// Error decoding an embedded value out of a framing-level
/// [`WorkflowArgument`]'s value bytes.
///
/// [`WorkflowArgument`]: waymark_proto::messages::WorkflowArgument
#[derive(Debug, thiserror::Error)]
#[error("decoding workflow argument value bytes for key {key:?}")]
pub struct DecodeArgumentError {
    /// The framing-level argument name the bytes belonged to.
    pub key: String,

    /// The decode failure.
    #[source]
    pub source: prost::DecodeError,
}

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
