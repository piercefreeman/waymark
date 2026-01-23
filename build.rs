fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/messages.proto");
    println!("cargo:rerun-if-changed=proto/ast.proto");
    println!("cargo:rerun-if-changed=proto/execution.proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Allow large enum variants in generated proto code
        .type_attribute(".", "#[allow(clippy::large_enum_variant)]")
        .compile(
            &[
                "proto/messages.proto",
                "proto/ast.proto",
                "proto/execution.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
