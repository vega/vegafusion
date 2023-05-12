#[macro_use]
extern crate lazy_static;
mod util;
use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;

fn medals() -> VegaFusionTable {
    VegaFusionTable::from_json(&json!([
        {"country": "Germany", "type": "gold", "count": 14, "is_gold": true},
        {"country": "Norway", "type": "gold", "count": 14, "is_gold": true},
        {"country": "Norway", "type": "silver", "count": 14, "is_gold": false},
        {"country": "Canada", "type": "copper", "count": 10, "is_gold": false},
        {"country": "Norway", "type": "bronze", "count": 11, "is_gold": false},
        {"country": "Germany", "type": "silver", "count": 10, "is_gold": false},
        {"country": "Germany", "type": "bronze", "count": 7, "is_gold": false},
        {"country": "Canada", "type": "gold", "count": 11, "is_gold": true},
        {"country": "Canada", "type": "silver", "count": 8, "is_gold": false},
        {"country": "Canada", "type": "bronze", "count": 10, "is_gold": false},
        {"country": "Canada", "type": null, "count": 5, "is_gold": null},
    ]))
    .unwrap()
}

fn colors() -> VegaFusionTable {
    VegaFusionTable::from_json(&json!([
        {"A": 1, "color": "red", "count": 1},
        {"A": 1, "color": "green", "count": 2},
        {"A": 1, "color": "", "count": 3},
        {"A": 2, "color": "red", "count": 4},
        {"A": 2, "color": "blue", "count": 5},
        {"A": 3, "color": "green", "count": 6},
        {"A": 3, "color": "red", "count": 7},
        {"A": 3, "color": "", "count": 8},
        {"A": 3, "color": "blue", "count": 9},
    ]))
    .unwrap()
}

#[cfg(test)]
mod test_pivot_with_group {
    use crate::medals;
    use crate::util::check::check_transform_evaluation;
    use rstest::rstest;
    use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
    use vegafusion_core::spec::transform::pivot::PivotTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest(
        op,
        limit,
        case(None, None),
        case(Some(AggregateOpSpec::Sum), None),
        case(Some(AggregateOpSpec::Sum), Some(2)),
        case(Some(AggregateOpSpec::Count), None),
        case(Some(AggregateOpSpec::Count), Some(3)),
        case(Some(AggregateOpSpec::Mean), None),
        case(Some(AggregateOpSpec::Mean), Some(4)),
        case(Some(AggregateOpSpec::Max), None),
        case(Some(AggregateOpSpec::Max), Some(10)),
        case(Some(AggregateOpSpec::Min), None),
        case(Some(AggregateOpSpec::Min), Some(0)),
        case(Some(AggregateOpSpec::Distinct), None)
    )]
    fn test(op: Option<AggregateOpSpec>, limit: Option<i32>) {
        let dataset = medals();

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: Some(vec!["country".to_string()]),
            limit,
            op,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let comp_config = Default::default();
        let eq_config = Default::default();

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }
}

#[cfg(test)]
mod test_pivot_no_group {
    use crate::medals;
    use crate::util::check::check_transform_evaluation;
    use rstest::rstest;
    use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
    use vegafusion_core::spec::transform::pivot::PivotTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest(
        op,
        limit,
        case(None, None),
        case(Some(AggregateOpSpec::Sum), None),
        case(Some(AggregateOpSpec::Sum), Some(2)),
        case(Some(AggregateOpSpec::Count), None),
        case(Some(AggregateOpSpec::Count), Some(3)),
        case(Some(AggregateOpSpec::Mean), None),
        case(Some(AggregateOpSpec::Mean), Some(4)),
        case(Some(AggregateOpSpec::Max), None),
        case(Some(AggregateOpSpec::Max), Some(10)),
        case(Some(AggregateOpSpec::Min), None),
        case(Some(AggregateOpSpec::Min), Some(0)),
        case(Some(AggregateOpSpec::Distinct), None)
    )]
    fn test(op: Option<AggregateOpSpec>, limit: Option<i32>) {
        let dataset = medals();

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: None,
            limit,
            op,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let comp_config = Default::default();
        let eq_config = Default::default();

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }
}

#[cfg(test)]
mod test_pivot_no_group_boolean {
    use crate::medals;
    use crate::util::check::check_transform_evaluation;
    use rstest::rstest;
    use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
    use vegafusion_core::spec::transform::pivot::PivotTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest(
        op,
        limit,
        case(None, None),
        case(Some(AggregateOpSpec::Sum), None),
        case(Some(AggregateOpSpec::Sum), Some(2)),
        case(Some(AggregateOpSpec::Count), None),
        case(Some(AggregateOpSpec::Count), Some(3)),
        case(Some(AggregateOpSpec::Mean), None),
        case(Some(AggregateOpSpec::Mean), Some(4)),
        case(Some(AggregateOpSpec::Max), None),
        case(Some(AggregateOpSpec::Max), Some(10)),
        case(Some(AggregateOpSpec::Min), None),
        case(Some(AggregateOpSpec::Min), Some(0)),
        case(Some(AggregateOpSpec::Distinct), None)
    )]
    fn test(op: Option<AggregateOpSpec>, limit: Option<i32>) {
        let dataset = medals();

        let pivot_spec = PivotTransformSpec {
            field: "is_gold".to_string(),
            value: "count".to_string(),
            groupby: None,
            limit,
            op,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let comp_config = Default::default();
        let eq_config = Default::default();

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }
}

#[cfg(test)]
mod test_pivot_with_empty_string {
    use crate::colors;
    use crate::util::check::eval_vegafusion_transforms;
    use rstest::rstest;
    use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
    use vegafusion_core::spec::transform::pivot::PivotTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;
    use vegafusion_runtime::expression::compiler::config::CompilationConfig;

    #[rstest(op, limit, case(Some(AggregateOpSpec::Sum), None))]
    fn test(op: Option<AggregateOpSpec>, limit: Option<i32>) {
        let dataset = colors();

        let pivot_spec = PivotTransformSpec {
            field: "color".to_string(),
            value: "count".to_string(),
            groupby: Some(vec!["A".to_string()]),
            limit,
            op,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let compilation_config = CompilationConfig::default();

        let (result_data, _result_signals) =
            eval_vegafusion_transforms(&dataset, transform_specs.as_slice(), &compilation_config);

        // Check representation of table
        assert_eq!(
            result_data.pretty_format(None).unwrap(),
            "\
+-----+------+-------+-----+---+
|     | blue | green | red | A |
+-----+------+-------+-----+---+
| 3.0 | 0.0  | 2.0   | 1.0 | 1 |
| 0.0 | 5.0  | 0.0   | 4.0 | 2 |
| 8.0 | 9.0  | 6.0   | 7.0 | 3 |
+-----+------+-------+-----+---+"
        );

        // Check that first column is a single space (" "), rather than an empty string.
        // Some SQL backends don't support empty string column names
        let name0 = result_data.schema.fields[0].name();
        assert_eq!(name0.as_str(), " ");
    }
}
