fn main() {
    println!("cargo:rerun-if-changed=proto/messages.proto");
    prost_build::compile_protos(&["proto/messages.proto"], &["proto"])
        .expect("failed to compile protos");
}
