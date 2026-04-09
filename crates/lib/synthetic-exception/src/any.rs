use crate::{ActionTimeout, ExecutorResume, RunnerExecutorError};

#[derive(Debug, strum::EnumDiscriminants)]
#[strum_discriminants(name(Type))]
#[strum_discriminants(derive(strum::EnumString))]
pub enum Any {
    ActionTimeout(ActionTimeout),
    ExecutorResume(ExecutorResume),
    RunnerExecutorError(RunnerExecutorError),
}

impl core::fmt::Display for Any {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Any::ActionTimeout(action_timeout) => action_timeout.fmt(f),
            Any::ExecutorResume(executor_resume) => executor_resume.fmt(f),
            Any::RunnerExecutorError(runner_executor_error) => runner_executor_error.fmt(f),
        }
    }
}

impl From<&Any> for crate::Value {
    fn from(value: &Any) -> Self {
        match value {
            Any::ActionTimeout(action_timeout) => action_timeout.into(),
            Any::ExecutorResume(executor_resume) => executor_resume.into(),
            Any::RunnerExecutorError(runner_executor_error) => runner_executor_error.into(),
        }
    }
}

impl Type {
    pub fn from_value(value: &serde_json::Value) -> Option<Type> {
        let map = value.as_object()?;
        let field = map.get("type")?;
        let s = field.as_str()?;
        let val = s.parse().ok()?;
        Some(val)
    }
}
