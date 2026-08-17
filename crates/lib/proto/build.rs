const PROTO_DIR: &str = "../../../proto";

fn if_feature_enabled(
    builder: tonic_prost_build::Builder,
    feature: &'static str,
    f: impl FnOnce(tonic_prost_build::Builder) -> tonic_prost_build::Builder,
) -> tonic_prost_build::Builder {
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
    let files = ["messages.proto", "ast.proto", "python_value.proto"];

    let proto_dir = std::path::Path::new(PROTO_DIR);

    let full_paths = files.map(|file| proto_dir.join(file));

    for full_path in &full_paths {
        println!("cargo:rerun-if-changed={}", full_path.display());
    }

    let mut builder = tonic_prost_build::configure();

    builder = if_feature_enabled(builder, "server", |b| b.build_server(true));
    builder = if_feature_enabled(builder, "client", |b| b.build_server(true));
    builder = if_feature_enabled(builder, "serde", |b| {
        // Enable serde support for persisted runtime state.
        b.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
    });

    builder
        // Allow large enum variants in generated proto code
        .type_attribute(".", "#[allow(clippy::large_enum_variant)]")
        // All well-known google.protobuf types come from prost-wkt-types,
        // which carries the serde support prost-types lacks. Compiling the
        // well-known types drops the built-in prost-types mapping so the
        // whole-domain extern path below can take over.
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::prost_wkt_types")
        .compile_protos(&full_paths[..], &[proto_dir.to_path_buf()])?;

    Ok(())
}
