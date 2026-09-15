use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// The environment variable that selects how the build includes the webapp.
const BUILD_ENABLE_ENV_VAR: &str = "WAYMARK_BUILD_WEBAPP_ENABLE";

enum Mode {
    Disabled,
    Required,
    BestEffort,
}

impl std::str::FromStr for Mode {
    type Err = InvalidModeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "required" => Ok(Self::Required),
            "best-effort" => Ok(Self::BestEffort),
            _ => Err(InvalidModeError(value.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("must be disabled, required or best-effort, not {0:?}")]
struct InvalidModeError(String);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest: PathBuf = envfury::must("CARGO_MANIFEST_DIR")?;
    let out_dir: PathBuf = envfury::must("OUT_DIR")?;
    let workspace = manifest.join("../../..");
    let placeholder = manifest.join("placeholder/index.html");
    let output = out_dir.join("webapp");

    println!("cargo::rerun-if-env-changed={BUILD_ENABLE_ENV_VAR}");
    println!("cargo::rerun-if-env-changed=CI");
    println!("cargo::rerun-if-changed={}", placeholder.display());
    println!("cargo::rustc-check-cfg=cfg(waymark_webapp_placeholder)");

    // CI builds must not pass with the placeholder by default.
    let ci: Option<String> = envfury::maybe("CI")?;
    let default_mode = if ci.is_some_and(|value| value == "true") {
        Mode::Required
    } else {
        Mode::BestEffort
    };

    let mode = match envfury::or(BUILD_ENABLE_ENV_VAR, default_mode) {
        Ok(mode) => mode,
        Err(error) => {
            println!("cargo::error={error}");
            return Ok(());
        }
    };

    let built = match mode {
        Mode::Disabled => false,
        Mode::Required => match build(&workspace, &output) {
            Ok(()) => true,
            Err(error) => {
                println!("cargo::error=building the webapp failed: {error}");
                println!(
                    "cargo::error=install Node.js (the version in .node-version) and run `make js-deps` at the repository root,"
                );
                println!(
                    "cargo::error=or set {BUILD_ENABLE_ENV_VAR} to best-effort or disabled to build with a placeholder page"
                );
                // Failing the script makes cargo show npm's captured output.
                return Err(error.into());
            }
        },
        Mode::BestEffort => match build(&workspace, &output) {
            Ok(()) => true,
            Err(error) => {
                println!(
                    "cargo::warning=building the webapp failed, embedding a placeholder page instead: {error}"
                );
                println!(
                    "cargo::warning=to include the webapp, install Node.js (the version in .node-version) and run `make js-deps` at the repository root"
                );
                false
            }
        },
    };

    if !built {
        // A failed build may have left part of its output behind.
        if output.exists() {
            fs::remove_dir_all(&output)?;
        }
        fs::create_dir_all(&output)?;
        fs::copy(&placeholder, output.join("index.html"))?;
        println!("cargo::rustc-cfg=waymark_webapp_placeholder");
    }

    Ok(())
}

fn build(workspace: &Path, output: &Path) -> Result<(), std::io::Error> {
    let webapp = workspace.join("js/app/web");
    // Installing the dependencies rewrites this file, so a placeholder from an
    // earlier failed build gets replaced.
    for input in [
        ".node-version",
        "package.json",
        "package-lock.json",
        "node_modules/.package-lock.json",
    ] {
        println!(
            "cargo::rerun-if-changed={}",
            workspace.join(input).display()
        );
    }
    for input in [
        "src",
        "index.html",
        "package.json",
        "tsconfig.json",
        "vite.config.ts",
    ] {
        println!("cargo::rerun-if-changed={}", webapp.join(input).display());
    }

    let status = Command::new("npm")
        .current_dir(&webapp)
        .args(["run", "build", "--", "--emptyOutDir", "--outDir"])
        .arg(output)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!("npm {status}")));
    }

    // The server embeds only index.html. Fail rather than ship missing assets.
    let files: Vec<_> = fs::read_dir(output)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<_, _>>()?;
    if files != ["index.html"] {
        return Err(std::io::Error::other(
            "the webapp must build to a single HTML file",
        ));
    }

    Ok(())
}
