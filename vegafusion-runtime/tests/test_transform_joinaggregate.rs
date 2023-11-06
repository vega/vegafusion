#[macro_use]
extern crate lazy_static;

mod util;

use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use util::equality::TablesEqualConfig;

use rstest::rstest;
use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
use vegafusion_core::spec::transform::joinaggregate::JoinAggregateTransformSpec;

use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::spec::values::Field;

mod test_joinaggregate_zero {
    use crate::*;
    use vegafusion_core::spec::transform::joinaggregate::JoinAggregateTransformSpec;

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
        case(AggregateOpSpec::Max)
    )]
    fn test(op: AggregateOpSpec) {
        let dataset = vega_json_dataset("penguins");

        let joinaggregate_spec = JoinAggregateTransformSpec {
            groupby: None,
            fields: vec![Some(Field::String("Beak Depth (mm)".to_string()))],
            ops: vec![op],
            as_: Some(vec![Some("agg".to_string())]),
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::JoinAggregate(joinaggregate_spec)];

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

mod test_joinaggregate_single {
    use crate::*;
    use vegafusion_core::spec::transform::joinaggregate::JoinAggregateTransformSpec;

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
        case(AggregateOpSpec::Max)
    )]
    fn test(op: AggregateOpSpec) {
        let dataset = vega_json_dataset("penguins");

        let joinaggregate_spec = JoinAggregateTransformSpec {
            groupby: Some(vec![Field::String("Species".to_string())]),
            fields: vec![Some(Field::String("Beak Depth (mm)".to_string()))],
            ops: vec![op],
            as_: Some(vec![Some("agg".to_string())]),
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::JoinAggregate(joinaggregate_spec)];

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

mod test_joinaggregate_multi {
    use crate::*;
    use vegafusion_core::spec::transform::filter::FilterTransformSpec;
    use vegafusion_core::spec::transform::joinaggregate::JoinAggregateTransformSpec;

    #[rstest(
        op1,
        op2,
        case(AggregateOpSpec::Count, AggregateOpSpec::Count),
        case(AggregateOpSpec::Valid, AggregateOpSpec::Missing),
        case(AggregateOpSpec::Missing, AggregateOpSpec::Valid),
        case(AggregateOpSpec::Distinct, AggregateOpSpec::Mean),
        case(AggregateOpSpec::Sum, AggregateOpSpec::Max),
        case(AggregateOpSpec::Mean, AggregateOpSpec::Sum),
        case(AggregateOpSpec::Average, AggregateOpSpec::Mean),
        case(AggregateOpSpec::Min, AggregateOpSpec::Average),
        case(AggregateOpSpec::Max, AggregateOpSpec::Min)
    )]
    fn test(op1: AggregateOpSpec, op2: AggregateOpSpec) {
        let dataset = vega_json_dataset("penguins");

        // Vega treats null grouping categories as a group whereas SQL does not.
        // For now, filter null values from grouping columns
        let filter_spec = FilterTransformSpec {
            expr: "isValid(datum['Sex'])".to_string(),
            extra: Default::default(),
        };

        let joinaggregate_spec = JoinAggregateTransformSpec {
            groupby: Some(vec![
                Field::String("Species".to_string()),
                Field::String("Island".to_string()),
                Field::String("Sex".to_string()),
            ]),
            fields: vec![
                Some(Field::String("Beak Depth (mm)".to_string())),
                Some(Field::String("Flipper Length (mm)".to_string())),
            ],
            ops: vec![op1, op2],
            as_: None,
            extra: Default::default(),
        };
        let transform_specs = vec![
            TransformSpec::Filter(filter_spec),
            TransformSpec::JoinAggregate(joinaggregate_spec),
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
}

#[test]
fn test_joinaggregate_count_star() {
    // Check for issues related to https://github.com/apache/arrow-datafusion/issues/6447
    let dataset = vega_json_dataset("penguins");

    let joinaggregate_spec = JoinAggregateTransformSpec {
        groupby: Some(vec![Field::String("Species".to_string())]),
        fields: vec![Some(Field::String("Beak Depth (mm)".to_string()))],
        ops: vec![AggregateOpSpec::Count],
        as_: Some(vec![Some("COUNT(*)".to_string())]),
        extra: Default::default(),
    };
    let transform_specs = vec![TransformSpec::JoinAggregate(joinaggregate_spec)];

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

        let aggregate_spec = JoinAggregateTransformSpec {
            groupby: Some(vec![Field::String("a".to_string())]),
            fields: vec![Some(Field::String("b".to_string()))],
            ops: vec![op],
            as_: None,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::JoinAggregate(aggregate_spec)];

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
