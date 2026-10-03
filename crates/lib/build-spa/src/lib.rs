//! Build an npm workspace's Vite SPA for embedding by a Rust build script.

use std::path::Path;

use xshell::{Shell, cmd};

/// Build `package_path` (relative to `workspace_root`) into an absolute output
/// directory, emitting Cargo rebuild directives for its source and configuration.
/// Install the root workspace's locked dependencies before calling this function.
pub fn build(
    workspace_root: &Path,
    package_path: &Path,
    output_directory: &Path,
) -> xshell::Result<()> {
    for input in [".node-version", "package.json", "package-lock.json"] {
        println!(
            "cargo::rerun-if-changed={}",
            workspace_root.join(input).display()
        );
    }
    for input in [
        "src",
        "index.html",
        "package.json",
        "tsconfig.json",
        "vite.config.ts",
    ] {
        println!(
            "cargo::rerun-if-changed={}",
            workspace_root.join(package_path).join(input).display()
        );
    }

    let shell = Shell::new()?;
    shell.change_dir(workspace_root);
    let command = if cfg!(windows) {
        cmd!(
            shell,
            "cmd /C npm run build --workspace {package_path} -- --emptyOutDir --outDir {output_directory}"
        )
    } else {
        cmd!(
            shell,
            "npm run build --workspace {package_path} -- --emptyOutDir --outDir {output_directory}"
        )
    };
    command.run()?;
    // Validate the SPA entry point; rust-embed will include the whole bundle.
    shell.read_file(output_directory.join("index.html"))?;
    Ok(())
}

#[cfg(test)]
mod tests;
