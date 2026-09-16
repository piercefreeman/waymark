use std::path::Path;

#[test]
fn runs_workspace_builds_and_propagates_failures() -> Result<(), Box<dyn std::error::Error>> {
    // Building without Node is supported, but this test needs npm; CI must run it.
    let ci = std::env::var("CI").is_ok_and(|value| value == "true");
    if !ci && !npm_runs() {
        eprintln!("skipped: npm is not available");
        return Ok(());
    }

    let shell = xshell::Shell::new()?;
    let temporary = shell.create_temp_dir()?;
    let workspace = temporary.path().join("workspace with spaces");
    let output = temporary.path().join("bundle with spaces");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/root");
    copy_tree(&fixture, &workspace)?;
    // The fixture has no dependencies; npm's install marker is all `build` checks.
    shell.write_file(workspace.join("node_modules/.package-lock.json"), "{}")?;
    let package = waymark_webapp_build::Package {
        workspace_root: &workspace,
        package_path: Path::new("packages/app"),
    };

    waymark_webapp_build::build(&package, &output)?;

    assert_eq!(
        Path::new(&shell.read_file(output.join("index.html"))?).canonicalize()?,
        workspace.join(package.package_path).canonicalize()?
    );
    assert!(output.join("assets/app.js").is_file());

    // A failed subprocess must not succeed using a previous build's entry point.
    let script = workspace.join(package.package_path).join("build.mjs");
    shell.write_file(&script, "process.exit(7);")?;
    assert!(waymark_webapp_build::build(&package, &output).is_err());

    shell.remove_path(&output)?;
    shell.write_file(&script, "")?;
    assert!(
        waymark_webapp_build::build(&package, &output).is_err(),
        "a successful command without index.html must fail"
    );

    Ok(())
}

#[test]
fn waits_for_missing_dependencies_without_running_npm() -> Result<(), Box<dyn std::error::Error>> {
    let shell = xshell::Shell::new()?;
    let temporary = shell.create_temp_dir()?;
    let workspace = temporary.path().join("workspace");
    let output = temporary.path().join("bundle");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/root");
    copy_tree(&fixture, &workspace)?;
    let package = waymark_webapp_build::Package {
        workspace_root: &workspace,
        package_path: Path::new("packages/app"),
    };
    let node_modules = workspace.join("node_modules");
    let sentinel = node_modules.join(".waymark-webapp-build-sentinel");

    let result = waymark_webapp_build::build(&package, &output);
    assert!(matches!(
        result,
        Err(waymark_webapp_build::BuildError::DependenciesMissing)
    ));
    assert!(sentinel.is_file());
    assert!(!output.exists(), "npm must not run");
    assert!(waymark_webapp_build::inputs(&package).contains(&node_modules));

    // A second failed build leaves the sentinel, and so node_modules, unchanged.
    let created = std::fs::metadata(&sentinel)?.modified()?;
    let result = waymark_webapp_build::build(&package, &output);
    assert!(result.is_err());
    assert_eq!(std::fs::metadata(&sentinel)?.modified()?, created);

    // Once installed, the install marker is watched instead.
    let install_marker = node_modules.join(".package-lock.json");
    shell.write_file(&install_marker, "{}")?;
    let inputs = waymark_webapp_build::inputs(&package);
    assert!(inputs.contains(&install_marker));
    assert!(!inputs.contains(&node_modules));

    Ok(())
}

#[test]
fn lists_inputs_inside_the_workspace() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/root");
    let package = waymark_webapp_build::Package {
        workspace_root: &workspace,
        package_path: Path::new("packages/app"),
    };

    let inputs = waymark_webapp_build::inputs(&package);

    assert!(inputs.contains(&workspace.join("package-lock.json")));
    assert!(inputs.contains(&workspace.join("packages/app/package.json")));
    for input in inputs {
        assert!(input.starts_with(&workspace), "{}", input.display());
    }
}

/// Whether an npm program can be spawned at all.
fn npm_runs() -> bool {
    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };

    std::process::Command::new(npm)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }

    Ok(())
}
