/// Raised when IR -> DAG conversion fails.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DagConversionError(pub String);

impl From<waymark_dag_validator::DagValidationError> for DagConversionError {
    fn from(value: waymark_dag_validator::DagValidationError) -> Self {
        Self(value.0)
    }
}
