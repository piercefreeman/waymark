use std::{env, path::PathBuf, process::Command};

fn main() {
    let workspace = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"))
        .join("../../..");
    let webapp = workspace.join("js/app/web");
    for input in [".node-version", "package.json", "package-lock.json"] {
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
        .expect("building the webapp requires Node.js and npm; run `make js-deps` first");
    assert!(
        status.success(),
        "webapp build failed; run `make js-deps` to install its dependencies"
    );

    assert!(
        output.join("index.html").is_file(),
        "webapp build must emit index.html"
    );
}
