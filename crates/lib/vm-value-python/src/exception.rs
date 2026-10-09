//! The Python exception as a value, and the classes the VM mints.
//!
//! Python's exception is one type in both roles the VM distinguishes - the
//! exception value in a register, defined here, and the raised exception in
//! flight, [`crate::RaisedException`] - so the conversions between the two
//! are identities. Its identity is the class name plus the class's bases,
//! most-derived first.

use crate::{RaisedException, ReadyValue, Value};

/// A Python exception.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Exception {
    /// The name of the raised class.
    pub type_id: String,

    /// The names of the raised class's bases in method-resolution order,
    /// most-derived first, `object` excluded.
    pub mro_type_ids: Vec<String>,

    /// Whatever the raiser put alongside: a dict of the particulars for a
    /// worker's exception, a message for the VM's own.
    pub details: Value,
}

/// A Python exception class as the VM mints it: the name and the bases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassSpec {
    /// The class name.
    pub type_id: &'static str,

    /// The names of the bases in method-resolution order, most-derived
    /// first, `object` excluded.
    pub mro_type_ids: &'static [&'static str],
}

impl ClassSpec {
    /// An instance of this class carrying the given details.
    pub fn exception(self, details: Value) -> Exception {
        Exception {
            type_id: self.type_id.to_owned(),
            mro_type_ids: self
                .mro_type_ids
                .iter()
                .map(|type_id| (*type_id).to_owned())
                .collect(),
            details,
        }
    }

    /// An instance of this class carrying a message.
    pub fn with_message(self, message: impl ToString) -> Exception {
        self.exception(Value::Ready(ReadyValue::String(message.to_string())))
    }
}

/// The Python exception classes the VM mints itself.
pub mod classes {
    use super::ClassSpec;

    /// `ZeroDivisionError`: a division by zero.
    pub const ZERO_DIVISION_ERROR: ClassSpec = ClassSpec {
        type_id: "ZeroDivisionError",
        mro_type_ids: &["ArithmeticError", "Exception", "BaseException"],
    };

    /// `OverflowError`: a result too large to be represented.
    pub const OVERFLOW_ERROR: ClassSpec = ClassSpec {
        type_id: "OverflowError",
        mro_type_ids: &["ArithmeticError", "Exception", "BaseException"],
    };

    /// `TypeError`: an operation applied to values of unsupported types.
    pub const TYPE_ERROR: ClassSpec = ClassSpec {
        type_id: "TypeError",
        mro_type_ids: &["Exception", "BaseException"],
    };

    /// `ValueError`: a value of the right type but an inappropriate value.
    pub const VALUE_ERROR: ClassSpec = ClassSpec {
        type_id: "ValueError",
        mro_type_ids: &["Exception", "BaseException"],
    };

    /// `AttributeError`: a failed attribute reference.
    pub const ATTRIBUTE_ERROR: ClassSpec = ClassSpec {
        type_id: "AttributeError",
        mro_type_ids: &["Exception", "BaseException"],
    };

    /// `IndexError`: a sequence index out of range.
    pub const INDEX_ERROR: ClassSpec = ClassSpec {
        type_id: "IndexError",
        mro_type_ids: &["LookupError", "Exception", "BaseException"],
    };

    /// `KeyError`: a missing mapping key.
    pub const KEY_ERROR: ClassSpec = ClassSpec {
        type_id: "KeyError",
        mro_type_ids: &["LookupError", "Exception", "BaseException"],
    };

    // The three runtime exceptions below have Python proxies in
    // `python/src/waymark/vm_exceptions.py`, so workflows can spell them.
    // The proxies mirror these entries - the same name, the same bases in
    // the same order - and must change together with them. Each side is
    // pinned to the same literals by its own test, the Rust one in
    // `tests/integration.rs` and `python/tests/test_vm_exceptions.py` on
    // the Python side, so a change to either table fails its own test.

    /// `ActionTimeout`: an action call attempt that timed out.
    ///
    /// Derives from `BaseException` directly: the timed-out attempt may
    /// still be running, so neither `except Exception:` nor a retry policy
    /// on `Exception` takes it - only one listing it, or `BaseException`.
    pub const ACTION_TIMEOUT: ClassSpec = ClassSpec {
        type_id: "ActionTimeout",
        mro_type_ids: &["BaseException"],
    };

    /// `ActionExecutionNotStarted`: an action call the worker never
    /// received - the dispatch was lost before it started, so nothing ran
    /// and a retry is safe. An ordinary exception.
    pub const ACTION_EXECUTION_NOT_STARTED: ClassSpec = ClassSpec {
        type_id: "ActionExecutionNotStarted",
        mro_type_ids: &["Exception", "BaseException"],
    };

    /// `ActionExecutionLost`: an action call the worker had when its
    /// execution was lost - how far it got is unknown, it may have run to
    /// completion.
    ///
    /// Derives from `BaseException` directly, like [`ACTION_TIMEOUT`] and
    /// for the same reason: retrying it is an explicit opt-in.
    pub const ACTION_EXECUTION_LOST: ClassSpec = ClassSpec {
        type_id: "ActionExecutionLost",
        mro_type_ids: &["BaseException"],
    };
}

impl waymark_vm_runtime_exception::ValueToRaisedException<RaisedException> for Exception {
    type Error = core::convert::Infallible;

    fn into_raised(self) -> Result<RaisedException, Self::Error> {
        Ok(self)
    }
}

impl waymark_vm_runtime_exception::RaisedExceptionToValue<RaisedException> for Exception {
    type Error = core::convert::Infallible;

    fn from_raised(raised: RaisedException) -> Result<Self, Self::Error> {
        Ok(raised)
    }
}
