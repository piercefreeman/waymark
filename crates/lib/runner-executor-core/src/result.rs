#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct UncheckedExecutionResult(pub serde_json::Value);

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ExecutionSuccess(pub serde_json::Value);

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ExecutionException(pub serde_json::Value);

pub type CheckedExecutionResult = Result<ExecutionSuccess, ExecutionException>;

impl UncheckedExecutionResult {
    pub fn check(self) -> CheckedExecutionResult {
        if crate::is_exception_value(&self.0) {
            return Err(ExecutionException(self.0));
        }
        Ok(ExecutionSuccess(self.0))
    }

    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

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
    pub fn into_unchecked(self) -> UncheckedExecutionResult {
        UncheckedExecutionResult(self.0)
    }
}

impl ExecutionException {
    pub fn into_unchecked(self) -> UncheckedExecutionResult {
        UncheckedExecutionResult(self.0)
    }
}
