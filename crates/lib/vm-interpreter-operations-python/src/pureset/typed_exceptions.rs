//! The typed exceptions for the Python pureset operation errors.

use super::error::{
    AsDictKeyError, AsScalarError, BinaryOperationError, DotOperationError, FromLengthError,
    IndexOperationError, LengthError, UnaryOperationError,
};

impl waymark_vm_runtime_exception::TypedException for AsScalarError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            AsScalarError::NotAScalar => waymark_vm_exception_type_ids::TYPE_ERROR,
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
            BinaryOperationError::UnsupportedOperation { .. } => {
                waymark_vm_exception_type_ids::TYPE_ERROR
            }
            BinaryOperationError::ResultOutOfBounds { .. } => {
                waymark_vm_exception_type_ids::OVERFLOW_ERROR
            }
            BinaryOperationError::DivisionByZero { .. } => {
                waymark_vm_exception_type_ids::ZERO_DIVISION_ERROR
            }
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
            UnaryOperationError::UnsupportedOperation { .. } => {
                waymark_vm_exception_type_ids::TYPE_ERROR
            }
            UnaryOperationError::ResultOutOfBounds { .. } => {
                waymark_vm_exception_type_ids::OVERFLOW_ERROR
            }
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
            LengthError::UnsupportedValue => waymark_vm_exception_type_ids::TYPE_ERROR,
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
            FromLengthError::ResultOutOfBounds => waymark_vm_exception_type_ids::OVERFLOW_ERROR,
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
            AsDictKeyError::UnsupportedKeyType => waymark_vm_exception_type_ids::TYPE_ERROR,
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
            IndexOperationError::UnsupportedOperation => waymark_vm_exception_type_ids::TYPE_ERROR,
            IndexOperationError::IndexOutOfBounds => waymark_vm_exception_type_ids::INDEX_ERROR,
            IndexOperationError::MissingKey => waymark_vm_exception_type_ids::KEY_ERROR,
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
            DotOperationError::UnsupportedOperation => waymark_vm_exception_type_ids::TYPE_ERROR,
            DotOperationError::MissingAttribute => waymark_vm_exception_type_ids::ATTRIBUTE_ERROR,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}
