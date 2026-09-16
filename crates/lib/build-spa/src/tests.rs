use std::path::Path;

#[test]
fn runs_workspace_builds_and_propagates_failures() -> Result<(), Box<dyn std::error::Error>> {
    let shell = xshell::Shell::new()?;
    let temporary = shell.create_temp_dir()?;
    let workspace = temporary.path().join("workspace with spaces");
    let output = temporary.path().join("bundle with spaces");
    let package = Path::new("js/app/web");
    shell.write_file(
        workspace.join("package.json"),
        r#"{"private":true,"workspaces":["js/app/*"]}"#,
    )?;
    shell.write_file(
        workspace.join(package).join("package.json"),
        r#"{"name":"test-webapp","private":true,"scripts":{"build":"node build.mjs"}}"#,
    )?;
    let script = workspace.join(package).join("build.mjs");
    shell.write_file(
        &script,
        r#"
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
assert.deepEqual(process.argv.slice(2, 4), ['--emptyOutDir', '--outDir']);
assert.equal(process.argv.length, 5);
const output = process.argv[4];
fs.mkdirSync(path.join(output, 'assets'), { recursive: true });
fs.writeFileSync(path.join(output, 'index.html'), process.cwd());
fs.writeFileSync(path.join(output, 'assets/app.js'), 'console.log("hello");');
"#,
    )?;

    super::build(&workspace, package, &output)?;
    assert_eq!(
        Path::new(&shell.read_file(output.join("index.html"))?).canonicalize()?,
        workspace.join(package).canonicalize()?
    );
    assert!(output.join("assets/app.js").is_file());

    // A failed subprocess must not succeed using a previous build's entry point.
    shell.write_file(&script, "process.exit(7);")?;
    assert!(super::build(&workspace, package, &output).is_err());

    shell.remove_path(&output)?;
    shell.write_file(&script, "")?;
    assert!(
        super::build(&workspace, package, &output).is_err(),
        "a successful command without index.html must fail"
    );
    Ok(())
}
