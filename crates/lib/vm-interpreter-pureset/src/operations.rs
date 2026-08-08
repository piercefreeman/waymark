//! Operations requirements.

/// A unifying trait for all operations requirements.
pub trait Operations<Value> {}

impl<T, Value> Operations<Value> for T {}

/// The exception model the operations have to satisfy for the errors of
/// the failing operations to be raised as runtime exceptions.
pub trait Exceptions<Value> {}

impl<T, Value> Exceptions<Value> for T {}

/// The exception model the value has to satisfy for the errors of the
/// failing operations to be raised as runtime exceptions.
pub trait ExceptionValue<Operations> {}

impl<T, Operations> ExceptionValue<Operations> for T {}
