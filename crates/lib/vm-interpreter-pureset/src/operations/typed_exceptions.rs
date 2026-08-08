//! The typed exceptions for the provided pureset operation errors.
//!
//! Only the errors of the *provided* vocabulary — the ones implemented
//! once for every variation — map to exception type ids here. The
//! semantic operations carry their own error types, and the variation
//! that defines them also defines their mapping.

use super::{ListAppendError, MakeDictError, MakeListError};

impl waymark_vm_runtime_exception::TypedException for MakeListError {
    type IntermediateDetails = String;

    fn into_intermediate_exception(
        self,
    ) -> waymark_vm_runtime_exception::Exception<Self::IntermediateDetails> {
        let type_id = match &self {
            MakeListError::NotListable => waymark_vm_exception_type_ids::TYPE_ERROR,
            MakeListError::ResultOutOfBounds => waymark_vm_exception_type_ids::OVERFLOW_ERROR,
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
            ListAppendError::NotListable => waymark_vm_exception_type_ids::TYPE_ERROR,
            ListAppendError::ResultOutOfBounds => waymark_vm_exception_type_ids::OVERFLOW_ERROR,
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
            MakeDictError::NotDictable => waymark_vm_exception_type_ids::TYPE_ERROR,
            MakeDictError::ResultOutOfBounds => waymark_vm_exception_type_ids::OVERFLOW_ERROR,
        };
        waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: self.to_string(),
        }
    }
}
