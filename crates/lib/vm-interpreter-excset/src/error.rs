/// The error for the [`crate::ExcSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Evaluating an `IsException` instruction failed.
    #[error("is exception: {0}")]
    IsException(#[source] crate::value::AsExceptionTypeIdError),

    /// Evaluating an `ExceptionDetails` instruction failed.
    #[error("exception details: {0}")]
    ExceptionDetails(#[source] crate::value::AsExceptionError),
}
