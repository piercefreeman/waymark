//! Build an npm workspace's Vite SPA for embedding by a Rust build script.

use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

use xshell::{Shell, cmd};

#[cfg(windows)]
mod windows;

/// npm, found once per process: searched for by full name on Windows, and
/// elsewhere left to the OS's own `PATH` search. Falls back to the bare name,
/// so a missing npm fails when the command runs.
static NPM: LazyLock<PathBuf> = LazyLock::new(|| {
    #[cfg(windows)]
    if let Some(npm) = windows::find_program("npm") {
        return npm;
    }

    PathBuf::from("npm")
});

/// npm's hidden lockfile inside `node_modules`. npm (7 and later) writes it on
/// every `npm install` and `npm ci` to record what is installed, so it exists
/// exactly when the dependencies are installed, and every install rewrites it.
/// `build` checks it before running npm, and `inputs` watches it to rebuild on
/// reinstalls.
const INSTALL_MARKER_FILE_NAME: &str = ".package-lock.json";

/// The file `build` leaves in `node_modules` while the dependencies are missing.
///
/// Cargo treats a missing `rerun-if-changed` path as changed on every build, so
/// watching the not-yet-written install marker would rerun the build script
/// (and rebuild everything that embeds the webapp) each time. Instead, `build`
/// creates `node_modules` holding only this file, and `inputs` watches that
/// directory: it stays unchanged until an install, which either replaces it
/// (`npm ci`) or adds to it (`npm install`), so the first build after
/// installing reruns on its own. The file is created once; rewriting it would
/// change the directory on every run.
const SENTINEL_FILE_NAME: &str = concat!(".", env!("CARGO_PKG_NAME"), "-sentinel");

/// An error building the package.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("the workspace's npm dependencies are not installed")]
    DependenciesMissing,

    #[error("creating the dependency sentinel {}: {source}", path.display())]
    SentinelCreation {
        path: PathBuf,

        #[source]
        source: std::io::Error,
    },

    #[error("npm build: {0}")]
    Npm(#[source] xshell::Error),
}

/// An npm workspace package to build.
pub struct Package<'a> {
    /// The root of the npm workspace, where its dependencies are installed.
    pub workspace_root: &'a Path,

    /// The package's path relative to `workspace_root`.
    pub package_path: &'a Path,
}

/// The files and directories whose changes affect the build of `package`: the
/// workspace's toolchain and npm install files, and the package's sources and
/// configuration.
pub fn inputs(package: &Package<'_>) -> Vec<PathBuf> {
    let Package {
        workspace_root,
        package_path,
    } = package;

    let workspace = [".node-version", "package.json", "package-lock.json"]
        .map(|input| workspace_root.join(input));

    // Installed: watch the install marker. Not installed: watch node_modules,
    // which `build` keeps in existence with the sentinel (see
    // `SENTINEL_FILE_NAME` for why the marker itself can't be watched then).
    let node_modules = workspace_root.join("node_modules");
    let install_marker = node_modules.join(INSTALL_MARKER_FILE_NAME);
    let install = if install_marker.is_file() {
        install_marker
    } else {
        node_modules
    };

    // Sources of other workspace packages the package links (`js/lib/*`) are
    // not watched; add them once the first such package exists.
    let sources = [
        "src",
        "index.html",
        "package.json",
        "tsconfig.json",
        "vite.config.ts",
    ]
    .map(|input| workspace_root.join(package_path).join(input));

    workspace
        .into_iter()
        .chain([install])
        .chain(sources)
        .collect()
}

/// Build `package` into an absolute output directory and check that the build
/// emitted its entry point. Install the workspace's locked dependencies before
/// calling this function; without them it fails without running npm.
pub fn build(package: &Package<'_>, output_directory: &Path) -> Result<(), BuildError> {
    let Package {
        workspace_root,
        package_path,
    } = package;

    // Without the dependencies, leave the sentinel for `inputs` to watch and
    // fail without running npm (see `SENTINEL_FILE_NAME`).
    let node_modules = workspace_root.join("node_modules");
    if !node_modules.join(INSTALL_MARKER_FILE_NAME).is_file() {
        let sentinel = node_modules.join(SENTINEL_FILE_NAME);
        if !sentinel.exists() {
            let created = std::fs::create_dir_all(&node_modules)
                .and_then(|()| std::fs::write(&sentinel, b""));
            if let Err(source) = created {
                return Err(BuildError::SentinelCreation {
                    path: sentinel,
                    source,
                });
            }
        }

        return Err(BuildError::DependenciesMissing);
    }

    let shell = Shell::new().map_err(BuildError::Npm)?;
    shell.change_dir(workspace_root);

    // `cmd!` interpolates plain variables only.
    let npm = &*NPM;
    cmd!(
        shell,
        "{npm} run build --workspace {package_path} -- --emptyOutDir --outDir {output_directory}"
    )
    .run()
    .map_err(BuildError::Npm)?;

    // Validate the SPA entry point; rust-embed will include the whole bundle.
    shell
        .read_file(output_directory.join("index.html"))
        .map_err(BuildError::Npm)?;

    Ok(())
}
