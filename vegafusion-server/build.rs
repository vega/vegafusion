fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_family = "unix")]
    std::env::set_var("PROTOC", protobuf_src::protoc());

    tonic_build::compile_protos("proto/helloworld.proto")?;
    Ok(())
}
