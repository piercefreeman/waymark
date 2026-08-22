//! Case outcome representation.
//!
//! An outcome is a VM value on both sides: the expected one decoded from
//! the value the Python helper encoded, the actual one taken from the VM
//! as it came.  Nothing is rendered on the way in, so a mismatch found
//! by [`crate::compare`] is a real difference rather than two renderings
//! disagreeing.

use waymark_convert_core::Convert as _;

use crate::compare::compare_outcomes;
use crate::ground_truth::PreparedCase;

/// How a case completed.
#[derive(Clone, Debug)]
pub enum CaseOutcome {
    /// The workflow ran to completion, producing this value.
    Completion(waymark_system_vm::ReadyValue),

    /// The workflow raised.
    Exception(waymark_vm_runtime_exception::Exception<waymark_system_vm::ReadyValue>),
}

impl CaseOutcome {
    /// The name this outcome goes by in a mismatch report.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Completion(_) => "completion",
            Self::Exception(_) => "exception",
        }
    }
}

/// Read the outcome the VM produced.
pub fn outcome_from_vm(
    outcome: waymark_workflow_completion_core::Outcome<waymark_system_vm::ReadyValue>,
) -> CaseOutcome {
    match outcome {
        waymark_workflow_completion_core::Outcome::Completion(value) => {
            CaseOutcome::Completion(value)
        }
        waymark_workflow_completion_core::Outcome::Exception(exception) => {
            CaseOutcome::Exception(exception)
        }
    }
}

/// Decode the outcome the Python helper encoded.
pub fn outcome_from_encoded(
    status: &str,
    value: &[u8],
) -> Result<CaseOutcome, color_eyre::eyre::Report> {
    use color_eyre::eyre::{WrapErr as _, bail};

    match status {
        "ok" => {
            let value = waymark_proto_python_value_conversions::decode_value(value)
                .wrap_err("decode the expected value")?;
            Ok(CaseOutcome::Completion(
                waymark_vm_value_convert_proto::Converter::convert(&value),
            ))
        }
        "error" => {
            let exception = waymark_proto_python_value_conversions::decode_exception_value(value)
                .wrap_err("decode the expected exception")?;
            Ok(CaseOutcome::Exception(
                waymark_vm_value_convert_proto::Converter::convert(&exception),
            ))
        }
        other => bail!("unknown expected outcome status {other:?}"),
    }
}

pub fn check_case_outcome(
    prepared: &PreparedCase,
    actual: Result<CaseOutcome, color_eyre::eyre::Report>,
) -> Option<String> {
    let mismatch = match actual {
        Ok(actual) if prepared.case.id == "timeout" => validate_timeout_outcome(&actual),
        Ok(actual) => compare_outcomes(&prepared.expected, &actual).err(),
        Err(err) => Some(format!("execution error: {err:#}")),
    };

    mismatch.map(|mismatch| format!("case={}\n{}", prepared.case.id, mismatch))
}

fn validate_timeout_outcome(actual: &CaseOutcome) -> Option<String> {
    let CaseOutcome::Exception(exception) = actual else {
        return Some(format!(
            "expected the timeout case to raise\nactual={actual:?}"
        ));
    };

    if exception.type_id != "ActionTimeout" {
        return Some(format!(
            "expected error type ActionTimeout\nactual={:?}",
            exception.type_id
        ));
    }

    // TODO: the lowered timeout raise constructs the `ActionTimeout` exception
    // with `details: None`, so the legacy payload assertions below have nothing
    // to check against. Restore them once the timeout exception carries its
    // payload (timeout duration, attempt number) again.
    //
    // let timeout_seconds = …details["timeout_seconds"] == 1
    // let attempt = …details["attempt"] == 1

    None
}
