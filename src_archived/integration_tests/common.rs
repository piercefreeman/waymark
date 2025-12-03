use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use tempfile::TempDir;
use tokio::{process::Command, time::timeout};

const SCRIPT_TIMEOUT: Duration = Duration::from_secs(30);

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
    let mut deps = vec![format!("rappel @ file://{}", repo_python.display())];
    deps.extend(requirements.iter().map(|s| s.to_string()));
    let deps_toml = deps
        .iter()
        .map(|dep| format!(r#""{dep}""#))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let pyproject = format!(
        r#"[project]
name = "rappel-integration"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    {deps_toml}
]
"#
    );
    fs::write(env_dir.path().join("pyproject.toml"), pyproject)?;
    run_shell(env_dir.path(), "uv sync", &[], None).await?;

    let mut run_envs = env_vars.to_vec();
    let mut python_paths = vec![
        repo_python.join("src"),
        repo_python.join("proto"),
        repo_python.clone(),
    ];
    if let Some(existing) = env::var_os("PYTHONPATH") {
        python_paths.extend(env::split_paths(&existing));
    }
    let pythonpath = env::join_paths(&python_paths)
        .context("failed to join python path entries")?
        .into_string()
        .map_err(|_| anyhow!("python path contains invalid unicode"))?;
    run_envs.push(("PYTHONPATH", pythonpath));
    run_shell(
        env_dir.path(),
        &format!("uv run python {entrypoint}"),
        &run_envs,
        Some(SCRIPT_TIMEOUT),
    )
    .await?;
    Ok(env_dir)
}

async fn run_shell(
    cwd: &Path,
    command: &str,
    envs: &[(&str, String)],
    timeout_limit: Option<Duration>,
) -> Result<()> {
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
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn `{command}`"))?;
    let wait_future = child.wait();
    let status = if let Some(limit) = timeout_limit {
        match timeout(limit, wait_future).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(anyhow!(
                    "command `{command}` timed out after {}s",
                    limit.as_secs()
                ));
            }
        }
    } else {
        wait_future.await?
    };
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("command `{command}` failed with {status}"))
    }
}
