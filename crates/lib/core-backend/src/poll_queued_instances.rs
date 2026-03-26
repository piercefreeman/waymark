pub trait Error {
    /// Get the classification for the error kind.
    ///
    /// Can be used by the consumer to make decisions on how to handle an error
    /// without coupling to the implementation details.
    fn kind(&self) -> ErrorKind;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// An error is indicating there were no instances.
    NoInstances,

    /// An error indicaing some internal error condition.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        // Infallible can never be constructed.
        match *self {}
    }
}
