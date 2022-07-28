/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::spec::transform::TransformSpecTrait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

macro_rules! unsupported_transforms {
    ( $( $name:ident ),* ) => {
        $(
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            #[serde(flatten)]
            pub extra: HashMap<String, Value>,
        }

        impl TransformSpecTrait for $name {
            fn supported(&self) -> bool { false }
        }

        )*
    };
}

unsupported_transforms!(
    CountpatternTransformSpec,
    ContourTransformSpec,
    CrossTransformSpec,
    CrossfilterTransformSpec,
    DensityTransformSpec,
    DotbinTransformSpec,
    FlattenTransformSpec,
    FoldTransformSpec,
    ForceTransformSpec,
    GeojsonTransformSpec,
    GeopathTransformSpec,
    GeopointTransformSpec,
    GeoshapeTransformSpec,
    GraticuleTransformSpec,
    HeatmapTransformSpec,
    IdentifierTransformSpec,
    IsocontourTransformSpec,
    KdeTransformSpec,
    Kde2dTransformSpec,
    LabelTransformSpec,
    LinkpathTransformSpec,
    LoessTransformSpec,
    NestTransformSpec,
    PackTransformSpec,
    PartitionTransformSpec,
    PieTransformSpec,
    PivotTransformSpec,
    QuantileTransformSpec,
    RegressionTransformSpec,
    ResolvefilterTransformSpec,
    SampleTransformSpec,
    StratifyTransformSpec,
    TreeTransformSpec,
    TreelinksTransformSpec,
    TreemapTransformSpec,
    VoronoiTransformSpec,
    WordcloudTransformSpec
);
