#[macro_use]
extern crate lazy_static;

mod util;

use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use util::equality::TablesEqualConfig;

use rstest::rstest;
use vegafusion_core::spec::transform::aggregate::{AggregateOp, AggregateTransformSpec};
use vegafusion_core::spec::transform::bin::{BinExtent, BinTransformSpec};
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::spec::values::{Field, SignalExpressionSpec};

mod test_aggregate_single {
    use crate::*;

    #[rstest(
        op,
        case(AggregateOp::Count),
        case(AggregateOp::Valid),
        case(AggregateOp::Missing),
        // Vega counts null as distinct category but DataFusion does not
        // case(AggregateOp::Distinct),
        case(AggregateOp::Sum),
        case(AggregateOp::Mean),
        case(AggregateOp::Average),
        case(AggregateOp::Min),
        case(AggregateOp::Max),
    )]
    fn test(op: AggregateOp) {
        let dataset = vega_json_dataset("penguins");
        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![Field::String("Species".to_string())],
            fields: vec![Some(Field::String("Beak Depth (mm)".to_string()))],
            ops: vec![op],
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        // Order of grouped rows is not defined, so set row_order to false
        let eq_config = TablesEqualConfig {
            row_order: false,
            ..Default::default()
        };

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }
}

mod test_aggregate_multi {
    use crate::*;

    #[rstest(
        op1, op2,
        // DataFusion error when two copies of Count(lit(0)) are included
        // case(AggregateOp::Count, AggregateOp::Count),
        case(AggregateOp::Valid, AggregateOp::Missing),
        case(AggregateOp::Missing, AggregateOp::Valid),
        // Vega counts null as distinct category but DataFusion does not
        // case(AggregateOp::Distinct),
        case(AggregateOp::Sum, AggregateOp::Max),
        case(AggregateOp::Mean, AggregateOp::Sum),
        case(AggregateOp::Average, AggregateOp::Mean),
        case(AggregateOp::Min, AggregateOp::Average),
        case(AggregateOp::Max, AggregateOp::Min),
    )]
    fn test(op1: AggregateOp, op2: AggregateOp) {
        let dataset = vega_json_dataset("penguins");
        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![
                Field::String("Species".to_string()),
                Field::String("Island".to_string()),
                Field::String("Sex".to_string()),
            ],
            fields: vec![
                Some(Field::String("Beak Depth (mm)".to_string())),
                Some(Field::String("Flipper Length (mm)".to_string())),
            ],
            ops: vec![op1, op2],
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        // Order of grouped rows is not defined, so set row_order to false
        let eq_config = TablesEqualConfig {
            row_order: false,
            ..Default::default()
        };

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }
}

#[test]
fn test_bin_aggregate() {
    let dataset = vega_json_dataset("penguins");

    // Note: use extent that doesn't result in -inf/inf until comparison logic can handle these
    // when row_order is false.
    let bin_spec = BinTransformSpec {
        field: Field::String("Body Mass (g)".to_string()),
        extent: BinExtent::Signal(SignalExpressionSpec {
            signal: "[0.0, 10000]".to_string(),
        }),
        signal: Some("my_bins".to_string()),
        as_: None,
        anchor: None,
        maxbins: None,
        base: None,
        step: None,
        steps: None,
        span: None,
        minstep: None,
        divide: None,
        nice: None,
        extra: Default::default(),
    };

    let aggregate_spec = AggregateTransformSpec {
        groupby: vec![Field::String("bin0".to_string())],
        fields: vec![
            Some(Field::String("Beak Depth (mm)".to_string())),
            Some(Field::String("Flipper Length (mm)".to_string())),
        ],
        ops: vec![AggregateOp::Mean, AggregateOp::Max],
        as_: None,
        cross: None,
        drop: None,
        key: None,
        extra: Default::default(),
    };

    let transform_specs = vec![
        TransformSpec::Bin(Box::new(bin_spec)),
        TransformSpec::Aggregate(aggregate_spec),
    ];

    let comp_config = Default::default();
    let eq_config = TablesEqualConfig {
        row_order: false,
        ..Default::default()
    };

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}
