#[derive(Debug, thiserror::Error)]
pub enum BinaryOpError {
    #[error("missing left")]
    LeftMissing,

    #[error("missing right")]
    RightMissing,
}

#[derive(Debug, thiserror::Error)]
pub enum UnaryOpError {
    #[error("missing operand")]
    Missing,
}

#[derive(Debug, thiserror::Error)]
pub enum DictEntryError {
    #[error("missing key")]
    MissingKey,

    #[error("missing value")]
    MissingValue,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexAccessError {
    #[error("missing object")]
    MissingObject,

    #[error("missing index")]
    MissingIndex,
}

#[derive(Debug, thiserror::Error)]
pub enum DotAccessError {
    #[error("missing object")]
    MissingObject,
}

#[derive(Debug, thiserror::Error)]
pub enum SpreadError {
    #[error("collection missing")]
    CollectionMissing,

    #[error("action missing")]
    ActionMissing,
}

#[derive(Debug, thiserror::Error)]
pub enum ExprToValueError<QueueActionCall> {
    #[error("queue action call: {0}")]
    QueueActionCall(#[source] QueueActionCall),

    #[error("binary op: {0}")]
    BinaryOp(#[from] BinaryOpError),

    #[error("unary op: {0}")]
    UnaryOp(#[from] UnaryOpError),

    #[error("dict entry: {0}")]
    DictEntry(#[from] DictEntryError),

    #[error("index access: {0}")]
    IndexAccess(#[from] IndexAccessError),

    #[error("dot access: {0}")]
    DotAccess(#[from] DotAccessError),

    #[error("spread: {0}")]
    Spread(#[from] SpreadError),
}
