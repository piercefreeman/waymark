//! Case preparation: Python inline ground truth and compiled IR.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;
use waymark_convert_core::Convert;
use waymark_proto::ast as ir;

use crate::cases::FixtureCase;
use crate::outcome::{CaseOutcome, canonicalize_outcome};

#[derive(Clone, Debug, Deserialize)]
struct HelperRegistration {
    workflow_name: String,
    workflow_version: String,
    ir_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
struct HelperOutput {
    expected: CaseOutcome,
    registration: HelperRegistration,
}

pub struct PreparedCase {
    pub case: FixtureCase,
    pub workflow_name: String,
    pub workflow_version: String,
    pub inputs: HashMap<String, waymark_system_vm::Value>,
    pub expected: CaseOutcome,
    pub program: waymark_vm_ast_old::Program,
}

pub fn prepare_case(repo_root: &Path, case: FixtureCase) -> Result<PreparedCase> {
    let kwargs_value: Value = serde_json::from_str(case.kwargs_json)
        .with_context(|| format!("parse kwargs JSON for case '{}'", case.id))?;
    let Value::Object(kwargs) = kwargs_value else {
        bail!("case '{}' kwargs JSON must be an object", case.id)
    };

    let helper = run_python_helper(repo_root, &case)?;

    let program = <ir::Program as prost::Message>::decode(&helper.registration.ir_bytes[..])
        .with_context(|| {
            format!(
                "decode IR bytes for case '{}' ({})",
                case.id, case.workflow_class
            )
        })?;
    let program = waymark_vm_ast_old_proto::convert(program).with_context(|| {
        format!(
            "convert IR to the VM AST for case '{}' ({})",
            case.id, case.workflow_class
        )
    })?;

    let mut inputs = HashMap::new();
    for (name, value) in kwargs {
        let value: waymark_system_vm::Value =
            waymark_vm_value_convert_json::Converter::convert(value);
        inputs.insert(name, value);
    }

    Ok(PreparedCase {
        case,
        workflow_name: helper.registration.workflow_name,
        workflow_version: helper.registration.workflow_version,
        inputs,
        expected: canonicalize_outcome(helper.expected),
        program,
    })
}

fn helper_python(repo_root: &Path) -> Result<PathBuf> {
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

fn run_python_helper(repo_root: &Path, case: &FixtureCase) -> Result<HelperOutput> {
    let helper_script = repo_root.join("scripts").join("fixture_ground_truth.py");
    let python = helper_python(repo_root)?;

    let output = Command::new(python)
        .arg(&helper_script)
        .arg("--module")
        .arg(case.module_name)
        .arg("--workflow-class")
        .arg(case.workflow_class)
        .arg("--kwargs-json")
        .arg(case.kwargs_json)
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("run python helper for case '{}'", case.id))?;

    if !output.status.success() {
        bail!(
            "python helper failed for case '{}'\nstdout:\n{}\nstderr:\n{}",
            case.id,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("decode python helper stdout for case '{}'", case.id))?;
    let payload = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .with_context(|| format!("python helper produced no payload for case '{}'", case.id))?;

    serde_json::from_str(payload)
        .with_context(|| format!("parse python helper JSON payload for case '{}'", case.id))
}
