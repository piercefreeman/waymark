//! The Python exception in flight: what is raised, matched by class, and
//! what the runtime's own failures lift to.
//!
//! The raised exception is the same type as the exception value,
//! [`crate::Exception`]; this module is the raised role of it: how a
//! handler's pattern matches it, how observability captures it, and how
//! each failing operation of the interpreter renders as one.

use nonempty_collections::NEVec;
use waymark_vm_interpreter_pureset::value::{
    AsDictKeyError, AsScalarError, BinaryOperationError, DotOperationError, FromLengthError,
    IndexOperationError, LengthError, ListAppendError, MakeDictError, MakeListError,
    UnaryOperationError,
};

use crate::Exception;
use crate::exception::classes;

/// The Python exception as raised: the exception value itself.
pub type RaisedException = Exception;

/// What a Python `except` clause catches, at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Pattern {
    /// A bare `except:`: everything.
    Any,

    /// `except A:` or `except (A, B):`: an exception of any listed class or
    /// of a class deriving from one.
    Classes(NEVec<String>),
}

/// The bytecode lists class names; none means a bare `except:`.
impl From<&Vec<String>> for Pattern {
    fn from(class_names: &Vec<String>) -> Self {
        match NEVec::try_from_vec(class_names.clone()) {
            Some(class_names) => Self::Classes(class_names),
            None => Self::Any,
        }
    }
}

impl waymark_vm_runtime_exception::HasMatchPattern for RaisedException {
    type Pattern = Pattern;
}

impl waymark_vm_runtime_exception::Match for RaisedException {
    fn matches(&self, pattern: &Self::Pattern) -> bool {
        match pattern {
            Pattern::Any => true,
            Pattern::Classes(class_names) => class_names.iter().any(|class_name| {
                *class_name == self.type_id || self.mro_type_ids.contains(class_name)
            }),
        }
    }
}

impl waymark_observability_vm_value_display::CaptureRaisedException for RaisedException {
    fn capture_raised_exception(
        &self,
    ) -> waymark_observability_vm_value_display::RaisedExceptionSummary {
        waymark_observability_vm_value_display::RaisedExceptionSummary {
            exception_type: self.type_id.clone(),
        }
    }
}

impl From<AsScalarError> for RaisedException {
    fn from(error: AsScalarError) -> Self {
        let class = match &error {
            AsScalarError::NotAScalar => classes::TYPE_ERROR,
        };
        class.with_message(error)
    }
}

impl From<BinaryOperationError> for RaisedException {
    fn from(error: BinaryOperationError) -> Self {
        let class = match &error {
            BinaryOperationError::UnsupportedOperation { .. } => classes::TYPE_ERROR,
            BinaryOperationError::ResultOutOfBounds { .. } => classes::OVERFLOW_ERROR,
            BinaryOperationError::DivisionByZero { .. } => classes::ZERO_DIVISION_ERROR,
        };
        class.with_message(error)
    }
}

impl From<UnaryOperationError> for RaisedException {
    fn from(error: UnaryOperationError) -> Self {
        let class = match &error {
            UnaryOperationError::UnsupportedOperation { .. } => classes::TYPE_ERROR,
            UnaryOperationError::ResultOutOfBounds { .. } => classes::OVERFLOW_ERROR,
        };
        class.with_message(error)
    }
}

impl From<LengthError> for RaisedException {
    fn from(error: LengthError) -> Self {
        let class = match &error {
            LengthError::UnsupportedValue => classes::TYPE_ERROR,
        };
        class.with_message(error)
    }
}

impl From<FromLengthError> for RaisedException {
    fn from(error: FromLengthError) -> Self {
        let class = match &error {
            FromLengthError::ResultOutOfBounds => classes::OVERFLOW_ERROR,
        };
        class.with_message(error)
    }
}

impl From<MakeListError> for RaisedException {
    fn from(error: MakeListError) -> Self {
        let class = match &error {
            MakeListError::NotListable => classes::TYPE_ERROR,
            MakeListError::ResultOutOfBounds => classes::OVERFLOW_ERROR,
        };
        class.with_message(error)
    }
}

impl From<ListAppendError> for RaisedException {
    fn from(error: ListAppendError) -> Self {
        let class = match &error {
            ListAppendError::NotListable => classes::TYPE_ERROR,
            ListAppendError::ResultOutOfBounds => classes::OVERFLOW_ERROR,
        };
        class.with_message(error)
    }
}

impl From<AsDictKeyError> for RaisedException {
    fn from(error: AsDictKeyError) -> Self {
        let class = match &error {
            AsDictKeyError::UnsupportedKeyType => classes::TYPE_ERROR,
        };
        class.with_message(error)
    }
}

impl From<MakeDictError> for RaisedException {
    fn from(error: MakeDictError) -> Self {
        let class = match &error {
            MakeDictError::NotDictable => classes::TYPE_ERROR,
            MakeDictError::ResultOutOfBounds => classes::OVERFLOW_ERROR,
        };
        class.with_message(error)
    }
}

impl From<IndexOperationError> for RaisedException {
    fn from(error: IndexOperationError) -> Self {
        let class = match &error {
            IndexOperationError::UnsupportedOperation => classes::TYPE_ERROR,
            IndexOperationError::IndexOutOfBounds => classes::INDEX_ERROR,
            IndexOperationError::MissingKey => classes::KEY_ERROR,
        };
        class.with_message(error)
    }
}

impl From<DotOperationError> for RaisedException {
    fn from(error: DotOperationError) -> Self {
        let class = match &error {
            DotOperationError::UnsupportedOperation => classes::TYPE_ERROR,
            DotOperationError::MissingAttribute => classes::ATTRIBUTE_ERROR,
        };
        class.with_message(error)
    }
}
