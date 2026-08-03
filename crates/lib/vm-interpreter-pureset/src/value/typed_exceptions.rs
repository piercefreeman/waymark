//! The typed exceptions for the pureset operation errors.

use super::{
    AsDictKeyError, AsScalarError, BinaryOperationError, DotOperationError, FromLengthError,
    IndexOperationError, LengthError, ListAppendError, MakeDictError, MakeListError,
    UnaryOperationError,
};

/// The runtime exception type identifier for a division by zero.
pub const ZERO_DIVISION_ERROR_TYPE_ID: &str = "ZeroDivisionError";

/// The runtime exception type identifier for an operation applied to values
/// of unsupported types.
pub const TYPE_ERROR_TYPE_ID: &str = "TypeError";

/// The runtime exception type identifier for a result too large to be
/// represented.
pub const OVERFLOW_ERROR_TYPE_ID: &str = "OverflowError";

/// The runtime exception type identifier for a sequence index out of range.
pub const INDEX_ERROR_TYPE_ID: &str = "IndexError";

/// The runtime exception type identifier for a missing mapping key.
pub const KEY_ERROR_TYPE_ID: &str = "KeyError";

/// The runtime exception type identifier for a failed attribute reference.
pub const ATTRIBUTE_ERROR_TYPE_ID: &str = "AttributeError";

impl waymark_vm_runtime_exception::TypedException for AsScalarError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            AsScalarError::NotAScalar => TYPE_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for BinaryOperationError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            BinaryOperationError::UnsupportedOperation { .. } => TYPE_ERROR_TYPE_ID,
            BinaryOperationError::ResultOutOfBounds { .. } => OVERFLOW_ERROR_TYPE_ID,
            BinaryOperationError::DivisionByZero { .. } => ZERO_DIVISION_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for UnaryOperationError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            UnaryOperationError::UnsupportedOperation { .. } => TYPE_ERROR_TYPE_ID,
            UnaryOperationError::ResultOutOfBounds { .. } => OVERFLOW_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for LengthError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            LengthError::UnsupportedValue => TYPE_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for FromLengthError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            FromLengthError::ResultOutOfBounds => OVERFLOW_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for MakeListError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            MakeListError::NotListable => TYPE_ERROR_TYPE_ID,
            MakeListError::ResultOutOfBounds => OVERFLOW_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for ListAppendError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            ListAppendError::NotListable => TYPE_ERROR_TYPE_ID,
            ListAppendError::ResultOutOfBounds => OVERFLOW_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for AsDictKeyError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            AsDictKeyError::UnsupportedKeyType => TYPE_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for MakeDictError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            MakeDictError::NotDictable => TYPE_ERROR_TYPE_ID,
            MakeDictError::ResultOutOfBounds => OVERFLOW_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for IndexOperationError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            IndexOperationError::UnsupportedOperation => TYPE_ERROR_TYPE_ID,
            IndexOperationError::IndexOutOfBounds => INDEX_ERROR_TYPE_ID,
            IndexOperationError::MissingKey => KEY_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}

impl waymark_vm_runtime_exception::TypedException for DotOperationError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            DotOperationError::UnsupportedOperation => TYPE_ERROR_TYPE_ID,
            DotOperationError::MissingAttribute => ATTRIBUTE_ERROR_TYPE_ID,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}
