#[macro_use]
extern crate lazy_static;

mod util;

use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use util::equality::TablesEqualConfig;

use rstest::rstest;
use vegafusion_core::spec::transform::aggregate::{AggregateOpSpec, AggregateTransformSpec};
use vegafusion_core::spec::transform::bin::{BinExtent, BinTransformSpec};
use vegafusion_core::spec::transform::filter::FilterTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::spec::values::{Field, SignalExpressionSpec};

mod test_aggregate_single {
    use crate::*;

    #[rstest(
        op,
        case(AggregateOpSpec::Count),
        case(AggregateOpSpec::Valid),
        case(AggregateOpSpec::Missing),
        case(AggregateOpSpec::Distinct),
        case(AggregateOpSpec::Sum),
        case(AggregateOpSpec::Mean),
        case(AggregateOpSpec::Average),
        case(AggregateOpSpec::Min),
        case(AggregateOpSpec::Max),
        case(AggregateOpSpec::Median),
        case(AggregateOpSpec::Q1),
        case(AggregateOpSpec::Q3)
    )]
    fn test(op: AggregateOpSpec) {
        let dataset = vega_json_dataset("penguins");
        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![Field::String("Species".to_string())],
            fields: Some(vec![Some(Field::String("Beak Depth (mm)".to_string()))]),
            ops: Some(vec![op]),
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
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
        op1,
        op2,
        case(AggregateOpSpec::Count, AggregateOpSpec::Count),
        case(AggregateOpSpec::Valid, AggregateOpSpec::Missing),
        case(AggregateOpSpec::Missing, AggregateOpSpec::Valid),
        case(AggregateOpSpec::Distinct, AggregateOpSpec::Valid),
        case(AggregateOpSpec::Sum, AggregateOpSpec::Max),
        case(AggregateOpSpec::Mean, AggregateOpSpec::Sum),
        case(AggregateOpSpec::Average, AggregateOpSpec::Mean),
        case(AggregateOpSpec::Min, AggregateOpSpec::Average),
        case(AggregateOpSpec::Max, AggregateOpSpec::Min),
        case(AggregateOpSpec::Median, AggregateOpSpec::Average),
        case(AggregateOpSpec::Q1, AggregateOpSpec::Q3)
    )]
    fn test(op1: AggregateOpSpec, op2: AggregateOpSpec) {
        let dataset = vega_json_dataset("penguins");
        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![
                Field::String("Species".to_string()),
                Field::String("Island".to_string()),
                Field::String("Sex".to_string()),
            ],
            fields: Some(vec![
                Some(Field::String("Beak Depth (mm)".to_string())),
                Some(Field::String("Flipper Length (mm)".to_string())),
            ]),
            ops: Some(vec![op1, op2]),
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
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
        fields: Some(vec![
            Some(Field::String("Beak Depth (mm)".to_string())),
            Some(Field::String("Flipper Length (mm)".to_string())),
        ]),
        ops: Some(vec![AggregateOpSpec::Min, AggregateOpSpec::Max]),
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
        row_order: true,
        ..Default::default()
    };

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

/// Test that the "as" column in a aggregate transform can have the same name as a Field,
/// then use the overwritten column in a filter expression.
/// Originally blocked on https://github.com/apache/arrow-datafusion/issues/1411
#[test]
fn test_aggregate_overwrite() {
    let dataset = vega_json_dataset("penguins");
    let aggregate_spec: AggregateTransformSpec = serde_json::from_value(serde_json::json!(
        {
            "groupby": ["Species"],
            "fields": ["Beak Depth (mm)"],
            "op": ["max"],
            "as": ["Beak Depth (mm)"]
        }
    ))
    .unwrap();
    let filter_spec: FilterTransformSpec = serde_json::from_value(serde_json::json!(
        {
            "expr": "isFinite(datum['Beak Depth (mm)'])",
        }
    ))
    .unwrap();

    let transform_specs = vec![
        TransformSpec::Aggregate(aggregate_spec),
        TransformSpec::Filter(filter_spec),
    ];

    let comp_config = Default::default();

    // Order of grouped rows is not defined, so set row_order to false
    let eq_config = TablesEqualConfig {
        row_order: true,
        ..Default::default()
    };

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

mod test_aggregate_with_nulls {
    use crate::*;
    use serde_json::json;
    use vegafusion_common::data::table::VegaFusionTable;

    #[rstest(
        op,
        case(AggregateOpSpec::Count),
        case(AggregateOpSpec::Valid),
        case(AggregateOpSpec::Missing),
        case(AggregateOpSpec::Distinct),
        case(AggregateOpSpec::Min),
        case(AggregateOpSpec::Max)
    )]
    fn test(op: AggregateOpSpec) {
        let dataset = VegaFusionTable::from_json(&json!(
            [
                {"SHIP": "A", "NULL_ORDER_IDS": null},
                {"SHIP": "B", "NULL_ORDER_IDS": "CA-2011-168312"},
                {"SHIP": "C", "NULL_ORDER_IDS": "CA-2011-131009"},
                {"SHIP": "D", "NULL_ORDER_IDS": "CA-2011-131009"},
                {"SHIP": "E", "NULL_ORDER_IDS": "CA-2011-131009"}
            ]
        ))
        .unwrap();

        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![Field::String("SHIP".to_string())],
            fields: Some(vec![Some(Field::String("NULL_ORDER_IDS".to_string()))]),
            ops: Some(vec![op]),
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
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

mod test_aggregate_strings {
    use crate::*;
    use serde_json::json;
    use vegafusion_common::data::table::VegaFusionTable;

    #[rstest(
        op,
        case(AggregateOpSpec::Count),
        case(AggregateOpSpec::Valid),
        case(AggregateOpSpec::Missing),
        case(AggregateOpSpec::Distinct),
        case(AggregateOpSpec::Min),
        case(AggregateOpSpec::Max)
    )]
    fn test(op: AggregateOpSpec) {
        let dataset = VegaFusionTable::from_json(&json!(
            [
                {"a": 1, "b": "A"},
                {"a": 1, "b": "BB"},
                {"a": 2, "b": "CCC"},
                {"a": 2, "b": "CCC"},
                {"a": 2, "b": "DDDD"}
            ]
        ))
        .unwrap();

        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![Field::String("a".to_string())],
            fields: Some(vec![Some(Field::String("b".to_string()))]),
            ops: Some(vec![op]),
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
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

mod test_aggregate_nulls {
    use crate::*;
    use serde_json::json;
    use vegafusion_common::data::table::VegaFusionTable;

    #[rstest(
        op,
        case(AggregateOpSpec::Count),
        case(AggregateOpSpec::Valid),
        case(AggregateOpSpec::Missing),
        case(AggregateOpSpec::Distinct),
        case(AggregateOpSpec::Sum),
        case(AggregateOpSpec::Mean),
        case(AggregateOpSpec::Average),
        case(AggregateOpSpec::Min),
        case(AggregateOpSpec::Max),
        case(AggregateOpSpec::Median),
        case(AggregateOpSpec::Q1),
        case(AggregateOpSpec::Q3)
    )]
    fn test(op: AggregateOpSpec) {
        let dataset = VegaFusionTable::from_json(&json!(
            [
                {"a": 1, "b": 1.0},
                {"a": 1, "b": null},
                {"a": 2, "b": null},
                {"a": 2, "b": null},
                {"a": 2, "b": null}
            ]
        ))
        .unwrap();

        let aggregate_spec = AggregateTransformSpec {
            groupby: vec![Field::String("a".to_string())],
            fields: Some(vec![Some(Field::String("b".to_string()))]),
            ops: Some(vec![op]),
            as_: None,
            cross: None,
            drop: None,
            key: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Aggregate(aggregate_spec)];

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
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
