/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
extern crate prost_build;

fn main() {
    let mut prost_config = prost_build::Config::new();
    let outdir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/proto/prost_gen");
    println!("outdir: {}", outdir);
    let prost_config = prost_config.out_dir(outdir);

    prost_config
        .compile_protos(
            &[
                "src/proto/expression.proto",
                "src/proto/transforms.proto",
                "src/proto/tasks.proto",
                "src/proto/errors.proto",
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
                "src/proto/services.proto",
            ],
            &["src/proto"],
        )
        .unwrap();
}
