fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/meta.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/meta.proto");
    Ok(())
}
