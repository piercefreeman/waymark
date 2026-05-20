//! Const value for [`waymark_vm_ast_old`].
//!
//! Provides lowering from the [`waymark_vm_ast_old::Literal`] and
//! binding to [`waymark_vm_value::Value`].

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

use typed_floats::NonNaNFinite;

/// A subset of [`waymark_vm_value::Value`] that can be lowered from
/// the [`waymark_vm_ast_old::Literal`].
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl From<&ConstValue> for waymark_vm_value::ReadyValue {
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
