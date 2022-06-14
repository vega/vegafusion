/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
extern crate prost_build;

fn main() {
    let mut prost_config = prost_build::Config::new();
    let outdir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/proto/prost_gen");
    println!("outdir: {}", outdir);
    let prost_config = prost_config.out_dir(outdir);

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
    let builder = tonic_build::configure();
    let outdir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/proto/tonic_gen");
    println!("outdir: {}", outdir);
    let builder = builder.out_dir(outdir);

    builder
        .compile(
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
