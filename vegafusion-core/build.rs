extern crate prost_build;

fn main() {
    // Setup protoc environment variable
    #[cfg(feature = "protobuf-src")]
    std::env::set_var("PROTOC", protobuf_src::protoc());

    let mut prost_config = prost_build::Config::new();

    prost_config
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "src/proto/expression.proto",
                "src/proto/transforms.proto",
                "src/proto/tasks.proto",
                "src/proto/errors.proto",
                "src/proto/pretransform.proto",
                "src/proto/services.proto",
            ],
            &["src/proto"],
        )
        .unwrap();

    #[cfg(feature = "tonic_support")]
    gen_tonic()
}

#[cfg(feature = "tonic_support")]
fn gen_tonic() {
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "src/proto/expression.proto",
                "src/proto/transforms.proto",
                "src/proto/tasks.proto",
                "src/proto/errors.proto",
                "src/proto/pretransform.proto",
                "src/proto/services.proto",
            ],
            &["src/proto"],
        )
        .unwrap();
}
