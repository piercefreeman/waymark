//! Case preparation: Python inline ground truth and compiled IR.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use color_eyre::eyre::{ContextCompat as _, WrapErr as _, bail};
use serde::Deserialize;
use waymark_convert_core::TryConvert as _;
use waymark_proto::ast as ir;

use crate::cases::FixtureCase;
use crate::outcome::{CaseOutcome, outcome_from_encoded};

#[derive(Clone, Debug, Deserialize)]
struct HelperRegistration {
    workflow_name: String,
    workflow_version: String,
    /// The compiled IR, hex-encoded.
    ir_bytes_hex: String,
}

/// The outcome the helper computed, as the value it encoded.
#[derive(Clone, Debug, Deserialize)]
struct HelperExpected {
    /// `ok` for a value, `error` for an exception.
    status: String,

    /// The encoded value or exception, hex-encoded.
    value_hex: String,
}

/// The helper's stdout envelope.
///
/// It carries names and opaque bytes only — every value in it is
/// encoded by the same encoder Python uses on the wire, so the envelope
/// itself never represents one.
#[derive(Clone, Debug, Deserialize)]
struct HelperOutput {
    expected: HelperExpected,
    registration: HelperRegistration,
}

/// Decode a hex string the helper emitted.
fn decode_hex(hex: &str, what: &str) -> Result<Vec<u8>, color_eyre::eyre::Report> {
    if !hex.len().is_multiple_of(2) {
        bail!("{what} is not a whole number of hex-encoded bytes")
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16)
                .wrap_err_with(|| format!("decode {what} as hex"))
        })
        .collect()
}

/// Encode a case's keyword arguments for the Python helper.
///
/// The arguments cross as the framing message the worker protocol uses,
/// so the helper reads them with the same reader a worker would.
fn encode_kwargs(case: &FixtureCase) -> Result<String, color_eyre::eyre::Report> {
    use prost::Message as _;

    let mut arguments = Vec::with_capacity(case.kwargs.len());
    for (name, kwarg) in case.kwargs {
        let value: waymark_proto::python_value::WorkflowArgumentValue =
            waymark_vm_value_convert_proto::Converter::try_convert(&kwarg.value())
                .wrap_err_with(|| format!("encode kwarg '{name}' of case '{}'", case.id))?;
        arguments.push(waymark_proto::messages::WorkflowArgument {
            key: (*name).to_owned(),
            value: waymark_proto_python_value_conversions::encode_workflow_argument_value(&value),
        });
    }

    let encoded = waymark_proto::messages::WorkflowArguments { arguments }.encode_to_vec();
    Ok(encoded.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub struct PreparedCase {
    pub case: FixtureCase,
    pub workflow_name: String,
    pub workflow_version: String,
    pub inputs: HashMap<String, waymark_system_vm::Value>,
    pub expected: CaseOutcome,
    pub program: waymark_vm_ast_old::Program,
}

pub fn prepare_case(
    repo_root: &Path,
    case: FixtureCase,
) -> Result<PreparedCase, color_eyre::eyre::Report> {
    let helper = run_python_helper(repo_root, &case)?;

    let ir_bytes = decode_hex(&helper.registration.ir_bytes_hex, "the IR bytes")?;
    let program = <ir::Program as prost::Message>::decode(&ir_bytes[..]).wrap_err_with(|| {
        format!(
            "decode IR bytes for case '{}' ({})",
            case.id, case.workflow_class
        )
    })?;
    let program = waymark_vm_ast_old_proto::convert(program).wrap_err_with(|| {
        format!(
            "convert IR to the VM AST for case '{}' ({})",
            case.id, case.workflow_class
        )
    })?;

    let mut inputs = HashMap::new();
    for (name, kwarg) in case.kwargs {
        inputs.insert(
            (*name).to_owned(),
            waymark_system_vm::Value::Ready(kwarg.value()),
        );
    }

    let expected_value = decode_hex(&helper.expected.value_hex, "the expected value")?;
    let expected = outcome_from_encoded(&helper.expected.status, &expected_value)
        .wrap_err_with(|| format!("read the expected outcome of case '{}'", case.id))?;

    Ok(PreparedCase {
        case,
        workflow_name: helper.registration.workflow_name,
        workflow_version: helper.registration.workflow_version,
        inputs,
        expected,
        program,
    })
}

fn helper_python(repo_root: &Path) -> Result<PathBuf, color_eyre::eyre::Report> {
    let python = repo_root.join(".venv").join("bin").join("python");
    if python.exists() {
        Ok(python)
    } else {
        bail!(
            "python helper interpreter not found at {}; run 'uv sync'",
            python.display()
        )
    }
}

fn run_python_helper(
    repo_root: &Path,
    case: &FixtureCase,
) -> Result<HelperOutput, color_eyre::eyre::Report> {
    let helper_script = repo_root.join("scripts").join("fixture_ground_truth.py");
    let python = helper_python(repo_root)?;

    let output = Command::new(python)
        .arg(&helper_script)
        .arg("--module")
        .arg(case.module_name)
        .arg("--workflow-class")
        .arg(case.workflow_class)
        .arg("--kwargs-hex-encoded")
        .arg(encode_kwargs(case)?)
        .current_dir(repo_root)
        .output()
        .wrap_err_with(|| format!("run python helper for case '{}'", case.id))?;

    if !output.status.success() {
        bail!(
            "python helper failed for case '{}'\nstdout:\n{}\nstderr:\n{}",
            case.id,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    let stdout = String::from_utf8(output.stdout)
        .wrap_err_with(|| format!("decode python helper stdout for case '{}'", case.id))?;
    let payload = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .with_context(|| format!("python helper produced no payload for case '{}'", case.id))?;

    serde_json::from_str(payload)
        .wrap_err_with(|| format!("parse python helper JSON payload for case '{}'", case.id))
}
