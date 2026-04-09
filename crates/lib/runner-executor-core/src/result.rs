//! The core types used in the executor and other system parts that don't need
//! to link with the whole executor subsystem but need to interact with it.

#![warn(missing_docs)]

/// An unchecked execution result.
///
/// Use when you have some execution result, but you didn't yet check what kind
/// of result it is (as in success vs exception).
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct UncheckedExecutionResult(pub serde_json::Value);

/// A successful execution result.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ExecutionSuccess(pub serde_json::Value);

/// A failed execution result.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ExecutionException(pub serde_json::Value);

/// An checked execution result.
///
/// Use when you have some execution result that you have already checked
/// (and know what kind it is) when you need to allow both success and failure
/// through a certain codepath while retaining the knowledge of which one it is.
pub type CheckedExecutionResult = Result<ExecutionSuccess, ExecutionException>;

impl UncheckedExecutionResult {
    /// Look and the underlying JSON of the unchecked execution result and
    /// determine from it whether it is a success or an exception.
    pub fn check(self) -> CheckedExecutionResult {
        if crate::is_exception_value(&self.0) {
            return Err(ExecutionException(self.0));
        }
        Ok(ExecutionSuccess(self.0))
    }

    /// Unwrap the inner JSON value.
    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

/// Go from a checked execution result to an unchecked one, essentially
/// erasing the type information on the result kind.
pub fn uncheck_execution_result(checked: CheckedExecutionResult) -> UncheckedExecutionResult {
    UncheckedExecutionResult(match checked {
        Ok(result) => result.0,
        Err(result) => result.0,
    })
}

impl From<ExecutionSuccess> for CheckedExecutionResult {
    fn from(value: ExecutionSuccess) -> Self {
        Ok(value)
    }
}

impl From<ExecutionException> for CheckedExecutionResult {
    fn from(value: ExecutionException) -> Self {
        Err(value)
    }
}

impl ExecutionSuccess {
    /// Go from an execution success to an unchecked execution result,
    /// essentially erasing the type information on the result kind.
    pub fn into_unchecked(self) -> UncheckedExecutionResult {
        UncheckedExecutionResult(self.0)
    }
}

impl ExecutionException {
    /// Go from an execution exception to an unchecked execution result,
    /// essentially erasing the type information on the result kind.
    pub fn into_unchecked(self) -> UncheckedExecutionResult {
        UncheckedExecutionResult(self.0)
    }
}
