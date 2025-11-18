use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, anyhow};
use tempfile::TempDir;
use tokio::process::Command;

pub async fn run_in_env(
    files: &[(&str, &str)],
    requirements: &[&str],
    env_vars: &[(&str, String)],
    entrypoint: &str,
) -> Result<TempDir> {
    let env_dir = TempDir::new().context("create python env dir")?;
    for (relative, contents) in files {
        let path = env_dir.path().join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents.trim_start())?;
    }
    let repo_python = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .canonicalize()?;
    let mut deps = vec![format!("carabiner @ file://{}", repo_python.display())];
    deps.extend(requirements.iter().map(|s| s.to_string()));
    let deps_toml = deps
        .iter()
        .map(|dep| format!(r#""{dep}""#))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let pyproject = format!(
        r#"[project]
name = "carabiner-integration"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    {deps_toml}
]
"#
    );
    fs::write(env_dir.path().join("pyproject.toml"), pyproject)?;
    run_shell(env_dir.path(), "uv sync", &[]).await?;

    let mut run_envs = env_vars.to_vec();
    let mut pythonpath = repo_python.display().to_string();
    if let Ok(existing) = env::var("PYTHONPATH")
        && !existing.is_empty()
    {
        pythonpath.push(':');
        pythonpath.push_str(&existing);
    }
    run_envs.push(("PYTHONPATH", pythonpath));
    run_shell(
        env_dir.path(),
        &format!("uv run python {entrypoint}"),
        &run_envs,
    )
    .await?;
    Ok(env_dir)
}

async fn run_shell(cwd: &Path, command: &str, envs: &[(&str, String)]) -> Result<()> {
    let mut cmd = Command::new("bash");
    cmd.arg("-lc")
        .arg(command)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (key, value) in envs {
        cmd.env(key, value);
    }
    let status = cmd.status().await?;
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("command `{command}` failed with {status}"))
    }
}
