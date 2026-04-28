#[derive(Debug, thiserror::Error)]
pub enum AddError {
    #[error("adding non-addable values")]
    NotAddable,

    #[error("addition result is out of bounds")]
    ResultOutOfBounds,
}

#[derive(Debug, thiserror::Error)]
pub enum IsTrueError {
    #[error("the value is not a boolean")]
    NotBoolean,
}

pub trait Value: Sized {
    fn add(a: &Self, b: &Self) -> Result<Self, AddError>;
    fn is_true(&self) -> Result<bool, IsTrueError>;
}
