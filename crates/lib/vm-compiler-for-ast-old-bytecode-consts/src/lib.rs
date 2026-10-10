//! The constants the compiler embeds in the bytecode for
//! [`waymark_vm_ast_old`]: values and exceptions.
//!
//! Provides lowering from the [`waymark_vm_ast_old::Literal`] and
//! binding to [`waymark_vm_value_python::Value`] and
//! [`waymark_vm_value_python::RaisedException`].

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use typed_floats::NonNaNFinite;
use waymark_vm_value_python::exception::ClassSpec;

/// A subset of [`waymark_vm_value_python::Value`] that can be lowered from
/// the [`waymark_vm_ast_old::Literal`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConstValue {
    /// Integer value.
    Int(i64),

    /// Non-NaN finite floating-point value.
    Float(NonNaNFinite),

    /// Boolean value.
    Bool(bool),

    /// String value.
    String(String),

    /// `None` value.
    None,
}

impl From<&ConstValue> for waymark_vm_value_python::ReadyValue {
    fn from(value: &ConstValue) -> Self {
        match value {
            ConstValue::Int(value) => Self::Int(*value),
            ConstValue::Float(value) => Self::Float(*value),
            ConstValue::Bool(value) => Self::Bool(*value),
            ConstValue::String(value) => Self::String(value.clone()),
            ConstValue::None => Self::None,
        }
    }
}

/// A [`waymark_vm_value_python::RaisedException`] as the bytecode embeds
/// one for the compiler's own raises: the class and a const details value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConstException {
    /// The name of the raised class.
    pub type_id: String,

    /// The names of the raised class's bases in method-resolution order,
    /// most-derived first, `object` excluded.
    pub mro_type_ids: Vec<String>,

    /// The details raised alongside.
    pub details: ConstValue,
}

/// What a handler lists in the bytecode: everything for a bare `except:`,
/// or the class names an `except` clause or a retry bracket lists.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConstExceptionPattern {
    /// A bare `except:`: everything.
    Any,

    /// The listed classes; none listed matches nothing.
    Classes(Vec<String>),
}

/// Lowers the class names an `except` clause or a retry bracket lists
/// into the bytecode's handler pattern.
pub fn lower_exception_pattern(class_names: &[String]) -> ConstExceptionPattern {
    ConstExceptionPattern::Classes(class_names.to_vec())
}

/// Lowers a bare `except:` into the bytecode's handler pattern.
pub fn lower_any_exception_pattern() -> ConstExceptionPattern {
    ConstExceptionPattern::Any
}

impl From<&ConstExceptionPattern> for waymark_vm_value_python::raised_exception::Pattern {
    fn from(pattern: &ConstExceptionPattern) -> Self {
        match pattern {
            ConstExceptionPattern::Any => Self::Any,
            ConstExceptionPattern::Classes(class_names) => Self::Classes(class_names.clone()),
        }
    }
}

impl ConstException {
    /// Lowers one of the compiler's own raises into the exception the
    /// bytecode embeds: the Python class it maps to, with no details.
    pub fn lower(
        exception: &waymark_vm_compiler_for_ast_old_core::lowering::CompilerEmittedException,
    ) -> Self {
        use waymark_vm_compiler_for_ast_old_core::lowering::CompilerEmittedException;
        use waymark_vm_value_python::exception::classes;

        let class = match exception {
            CompilerEmittedException::UnpackMismatch => classes::VALUE_ERROR,
            CompilerEmittedException::ActionTimeout => classes::ACTION_TIMEOUT,
        };
        Self::new(class, ConstValue::None)
    }

    /// An instance of the class carrying the given const details.
    pub fn new(class: ClassSpec, details: ConstValue) -> Self {
        Self {
            type_id: class.type_id.to_owned(),
            mro_type_ids: class
                .mro_type_ids
                .iter()
                .map(|type_id| (*type_id).to_owned())
                .collect(),
            details,
        }
    }
}

impl From<&ConstException> for waymark_vm_value_python::RaisedException {
    fn from(exception: &ConstException) -> Self {
        Self {
            type_id: exception.type_id.clone(),
            mro_type_ids: exception.mro_type_ids.clone(),
            details: waymark_vm_value_python::Value::Ready(
                waymark_vm_value_python::ReadyValue::from(&exception.details),
            ),
        }
    }
}

/// Errors produced while lowering literals.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LoweringError {
    /// The float was an invalid value.
    #[error("invalid float: {0}")]
    InvalidFloat(#[source] typed_floats::InvalidNumber),
}

impl ConstValue {
    /// Lowers one [`waymark_vm_ast_old::Literal`] into a [`ConstValue`].
    pub fn lower(literal: &waymark_vm_ast_old::Literal) -> Result<Self, LoweringError> {
        use waymark_vm_ast_old::Literal;
        match literal {
            Literal::Int(value) => Ok(ConstValue::Int(*value)),
            Literal::Float(value) => {
                let value = (*value).try_into().map_err(LoweringError::InvalidFloat)?;
                Ok(ConstValue::Float(value))
            }
            Literal::String(value) => Ok(ConstValue::String(value.clone())),
            Literal::Bool(value) => Ok(ConstValue::Bool(*value)),
            Literal::None => Ok(ConstValue::None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConstValue, LoweringError};

    #[test]
    fn rejects_non_finite_float_literals() {
        let error = ConstValue::lower(&waymark_vm_ast_old::Literal::Float(f64::NAN))
            .expect_err("non-finite floats should fail");

        assert!(matches!(error, LoweringError::InvalidFloat(_)));
    }
}
