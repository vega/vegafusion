extern crate prost_build;

fn main() {
    let mut prost_config = prost_build::Config::new();
    let outdir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/proto/gen");
    println!("outdir: {}", outdir);
    let prost_config = prost_config.out_dir(outdir);

    prost_config
        .compile_protos(
            &["src/proto/expression.proto", "src/proto/transforms.proto"],
            &["src/proto"],
        )
        .unwrap();
}
