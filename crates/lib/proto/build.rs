const PROTO_DIR: &str = "../../../proto";

fn if_feature_enabled(
    builder: tonic_build::Builder,
    feature: &'static str,
    f: impl FnOnce(tonic_build::Builder) -> tonic_build::Builder,
) -> tonic_build::Builder {
    let feature_status = std::env::var_os(format!(
        "CARGO_FEATURE_{}",
        feature.replace('-', "_").to_uppercase()
    ));

    if feature_status.is_none() {
        return builder;
    }

    f(builder)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = ["messages.proto", "ast.proto", "execution.proto"];

    let proto_dir = std::path::Path::new(PROTO_DIR);

    let full_paths = files.map(|file| proto_dir.join(file));

    for full_path in &full_paths {
        println!("cargo:rerun-if-changed={}", full_path.display());
    }

    let mut builder = tonic_build::configure();

    builder = if_feature_enabled(builder, "server", |b| b.build_server(true));
    builder = if_feature_enabled(builder, "client", |b| b.build_server(true));
    builder = if_feature_enabled(builder, "serde", |b| {
        // Enable serde support for persisted runtime state.
        b.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
    });

    builder
        // Allow large enum variants in generated proto code
        .type_attribute(".", "#[allow(clippy::large_enum_variant)]")
        .compile(&full_paths[..], &[PROTO_DIR])?;

    Ok(())
}
