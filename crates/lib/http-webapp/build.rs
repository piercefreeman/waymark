use std::{env, fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = Path::new(&env::var("CARGO_MANIFEST_DIR")?).join("../../..");
    let output = Path::new(&env::var("OUT_DIR")?).join("webapp");
    waymark_build_spa::build(&workspace, Path::new("js/app/web"), &output)?;
    // rust-embed requires a literal folder attribute; use Cargo's resolved path.
    fs::write(
        Path::new(&env::var("OUT_DIR")?).join("assets.rs"),
        format!("#[derive(Embed)]\n#[folder = {output:?}]\nstruct Assets;\n"),
    )?;
    Ok(())
}
