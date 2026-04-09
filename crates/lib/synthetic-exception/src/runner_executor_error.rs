/// A type that represents a runner executor error and a synthetic exception.
#[derive(Debug, Clone)]
pub struct RunnerExecutorError(pub String);

impl core::fmt::Display for RunnerExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&RunnerExecutorError> for crate::Value {
    fn from(value: &RunnerExecutorError) -> Self {
        Self::new("RunnerExecutorError", value)
    }
}
