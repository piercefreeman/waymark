//! The exception type identifiers for the built-in runtime exceptions.
//!
//! The single source of truth for the type id spellings shared by
//! the producers (the compiler lowerings and the interpreters) and
//! the exception handler matching.

#![warn(missing_docs)]

/// The runtime exception type identifier for a division by zero.
pub const ZERO_DIVISION_ERROR: &str = "ZeroDivisionError";

/// The runtime exception type identifier for an operation applied to values
/// of unsupported types.
pub const TYPE_ERROR: &str = "TypeError";

/// The runtime exception type identifier for a result too large to be
/// represented.
pub const OVERFLOW_ERROR: &str = "OverflowError";

/// The runtime exception type identifier for a sequence index out of range.
pub const INDEX_ERROR: &str = "IndexError";

/// The runtime exception type identifier for a missing mapping key.
pub const KEY_ERROR: &str = "KeyError";

/// The runtime exception type identifier for a failed attribute reference.
pub const ATTRIBUTE_ERROR: &str = "AttributeError";

/// The runtime exception type identifier for a value of the right type but
/// an inappropriate value.
pub const VALUE_ERROR: &str = "ValueError";

/// The runtime exception type identifier for an action call that timed out.
pub const ACTION_TIMEOUT: &str = "ActionTimeout";

/// The runtime exception type identifier for an action call whose
/// execution was lost: the worker died and no result will ever come
/// from that attempt.
///
/// Raised by the runtime so the program's own policy — a compiled-in
/// retry, a user `except`, or nothing — decides what the loss means.
pub const EXECUTION_LOST: &str = "ExecutionLost";
