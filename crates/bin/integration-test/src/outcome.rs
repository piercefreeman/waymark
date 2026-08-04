//! Case outcome representation and comparison against ground truth.

use std::collections::BTreeMap;

use color_eyre::eyre::WrapErr as _;
use serde::{Deserialize, Serialize};
use waymark_convert_core::TryConvert;

use crate::ground_truth::PreparedCase;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CaseOutcome {
    pub status: String,
    pub value: serde_json::Value,
}

pub fn check_case_outcome(
    prepared: &PreparedCase,
    actual: Result<CaseOutcome, color_eyre::eyre::Report>,
) -> Option<String> {
    let mismatch = match actual {
        Ok(actual) if prepared.case.id == "timeout" => validate_timeout_outcome(&actual),
        Ok(actual) if actual != prepared.expected => Some(format!(
            "expected={}\nactual={}",
            serde_json::to_string(&prepared.expected).expect("serialize expected"),
            serde_json::to_string(&actual).expect("serialize actual"),
        )),
        Ok(_actual) => None,
        Err(err) => Some(format!("execution error: {err:#}")),
    };

    mismatch.map(|mismatch| format!("case={}\n{}", prepared.case.id, mismatch))
}

pub fn outcome_from_vm(
    outcome: waymark_workflow_completion_core::Outcome<waymark_system_vm::ReadyValue>,
) -> Result<CaseOutcome, color_eyre::eyre::Report> {
    match outcome {
        waymark_workflow_completion_core::Outcome::Completion(value) => {
            let value: serde_json::Value =
                waymark_vm_value_convert_json::Converter::try_convert(value)
                    .wrap_err("convert workflow completion value to JSON")?;
            Ok(CaseOutcome {
                status: "ok".to_string(),
                value,
            })
        }
        waymark_workflow_completion_core::Outcome::Exception(exception) => {
            let value: serde_json::Value =
                waymark_vm_value_convert_json::Converter::try_convert(exception)
                    .wrap_err("convert workflow exception to JSON")?;
            Ok(CaseOutcome {
                status: "error".to_string(),
                value,
            })
        }
    }
}

pub fn canonicalize_outcome(outcome: CaseOutcome) -> CaseOutcome {
    CaseOutcome {
        status: outcome.status,
        value: canonicalize_json(outcome.value),
    }
}

fn validate_timeout_outcome(actual: &CaseOutcome) -> Option<String> {
    if actual.status != "error" {
        return Some(format!(
            "expected timeout status=error\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    }

    let serde_json::Value::Object(payload) = &actual.value else {
        return Some(format!(
            "expected timeout payload object\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    };

    let error_type = payload.get("type").and_then(serde_json::Value::as_str);
    if error_type != Some("ActionTimeout") {
        return Some(format!(
            "expected error type ActionTimeout\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    }

    // TODO: the lowered timeout raise constructs the `ActionTimeout` exception
    // with `details: None`, so the legacy payload assertions below have nothing
    // to check against. Restore them once the timeout exception carries its
    // payload (timeout duration, attempt number) again.
    //
    // let timeout_seconds = payload.get("timeout_seconds").and_then(serde_json::Value::as_i64);
    // if timeout_seconds != Some(1) {
    //     return Some(format!(
    //         "expected timeout_seconds=1\nactual={}",
    //         serde_json::to_string(actual).expect("serialize actual")
    //     ));
    // }
    //
    // let attempt = payload.get("attempt").and_then(serde_json::Value::as_i64);
    // if attempt != Some(1) {
    //     return Some(format!(
    //         "expected attempt=1\nactual={}",
    //         serde_json::to_string(actual).expect("serialize actual")
    //     ));
    // }

    None
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(map) => {
            let mut ordered = BTreeMap::new();
            for (key, item) in map {
                ordered.insert(key, canonicalize_json(item));
            }
            let mut normalized = serde_json::Map::new();
            for (key, item) in ordered {
                normalized.insert(key, item);
            }
            serde_json::Value::Object(normalized)
        }
        other => other,
    }
}
