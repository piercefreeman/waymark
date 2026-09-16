use std::{env, fs, path::PathBuf, process::Command};

fn main() {
    let webapp = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"))
        .join("../../../webapp");
    println!("cargo::rerun-if-changed={}", webapp.join("../.node-version").display());
    for input in [
        "src",
        "index.html",
        "package.json",
        "package-lock.json",
        "tsconfig.json",
        "vite.config.ts",
    ] {
        println!("cargo::rerun-if-changed={}", webapp.join(input).display());
    }

    let output = PathBuf::from(env::var_os("OUT_DIR").expect("output directory")).join("webapp");
    let mut command = if cfg!(windows) {
        let mut command = Command::new("cmd");
        command.args(["/C", "npm"]);
        command
    } else {
        Command::new("npm")
    };
    let status = command
        .current_dir(&webapp)
        .args(["run", "build", "--", "--emptyOutDir", "--outDir"])
        .arg(&output)
        .status()
        .expect("building the webapp requires Node.js and npm; run `make webapp-deps` first");
    assert!(
        status.success(),
        "webapp build failed; run `make webapp-deps` to install its dependencies"
    );

    // The server embeds only index.html. Fail rather than ship missing assets.
    let files: Vec<_> = fs::read_dir(&output)
        .expect("webapp output directory")
        .map(|entry| entry.expect("webapp output entry").file_name())
        .collect();
    assert_eq!(
        files,
        ["index.html"],
        "webapp must build to a single HTML file"
    );
}
