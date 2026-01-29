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
+---+------+-------+-----+---+
|   | blue | green | red | A |
+---+------+-------+-----+---+
| 3 |      | 2     | 1   | 1 |
|   | 5    |       | 4   | 2 |
| 8 | 9    | 6     | 7   | 3 |
+---+------+-------+-----+---+"
        );

        // Check that first column is a single space (" "), rather than an empty string.
        // Some SQL backends don't support empty string column names
        let name0 = result_data.schema.fields[0].name();
        assert_eq!(name0.as_str(), " ");
    }
}

/// Test that pivot transform works correctly with different Arrow string types.
/// This is important because polars uses Utf8View for string columns, while the
/// original pivot transform only handled Utf8 (StringArray).
/// See: https://github.com/vega/vegafusion/issues/572
#[cfg(test)]
mod test_pivot_with_different_string_types {
    use crate::util::check::eval_vegafusion_transforms;
    use std::sync::Arc;
    use vegafusion_common::arrow::array::{
        ArrayRef, Float64Array, LargeStringArray, StringArray, StringViewArray, UInt64Array,
    };
    use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
    use vegafusion_common::arrow::record_batch::RecordBatch;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_common::data::ORDER_COL;
    use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
    use vegafusion_core::spec::transform::pivot::PivotTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;
    use vegafusion_runtime::expression::compiler::config::CompilationConfig;

    /// Create a test table with a pivot column using the specified string array type
    fn create_medals_table_with_string_type(string_array: ArrayRef) -> VegaFusionTable {
        let order_col: ArrayRef = Arc::new(UInt64Array::from(vec![0, 1, 2, 3, 4]));
        let country: ArrayRef = Arc::new(StringArray::from(vec![
            "Germany", "Norway", "Canada", "Germany", "Canada",
        ]));
        let count: ArrayRef = Arc::new(Float64Array::from(vec![14.0, 14.0, 10.0, 10.0, 8.0]));

        let schema = Arc::new(Schema::new(vec![
            Field::new(ORDER_COL, DataType::UInt64, true),
            Field::new("type", string_array.data_type().clone(), true),
            Field::new("country", DataType::Utf8, true),
            Field::new("count", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![order_col, string_array, country, count],
        )
        .unwrap();

        VegaFusionTable::try_new(schema, vec![batch]).unwrap()
    }

    #[test]
    fn test_pivot_with_utf8view_strings() {
        // Create Utf8View array (as polars would produce)
        let type_array: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("gold"),
            Some("gold"),
            Some("copper"),
            Some("silver"),
            Some("silver"),
        ]));

        let dataset = create_medals_table_with_string_type(type_array);

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: Some(vec!["country".to_string()]),
            limit: None,
            op: Some(AggregateOpSpec::Sum),
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let compilation_config = CompilationConfig::default();
        let (result_data, _result_signals) =
            eval_vegafusion_transforms(&dataset, transform_specs.as_slice(), &compilation_config);

        // Should have columns: copper, gold, silver, country (plus order col)
        let field_names: Vec<_> = result_data
            .schema
            .fields
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"gold"));
        assert!(field_names.contains(&"silver"));
        assert!(field_names.contains(&"copper"));
        assert!(field_names.contains(&"country"));
    }

    #[test]
    fn test_pivot_with_large_utf8_strings() {
        // Create LargeUtf8 array
        let type_array: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("gold"),
            Some("gold"),
            Some("copper"),
            Some("silver"),
            Some("silver"),
        ]));

        let dataset = create_medals_table_with_string_type(type_array);

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: Some(vec!["country".to_string()]),
            limit: None,
            op: Some(AggregateOpSpec::Sum),
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let compilation_config = CompilationConfig::default();
        let (result_data, _result_signals) =
            eval_vegafusion_transforms(&dataset, transform_specs.as_slice(), &compilation_config);

        // Should have columns: copper, gold, silver, country
        let field_names: Vec<_> = result_data
            .schema
            .fields
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"gold"));
        assert!(field_names.contains(&"silver"));
        assert!(field_names.contains(&"copper"));
        assert!(field_names.contains(&"country"));
    }

    #[test]
    fn test_pivot_with_standard_utf8_strings() {
        // Create standard Utf8 array (baseline test)
        let type_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("gold"),
            Some("gold"),
            Some("copper"),
            Some("silver"),
            Some("silver"),
        ]));

        let dataset = create_medals_table_with_string_type(type_array);

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: Some(vec!["country".to_string()]),
            limit: None,
            op: Some(AggregateOpSpec::Sum),
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Pivot(pivot_spec)];

        let compilation_config = CompilationConfig::default();
        let (result_data, _result_signals) =
            eval_vegafusion_transforms(&dataset, transform_specs.as_slice(), &compilation_config);

        // Should have columns: copper, gold, silver, country
        let field_names: Vec<_> = result_data
            .schema
            .fields
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"gold"));
        assert!(field_names.contains(&"silver"));
        assert!(field_names.contains(&"copper"));
        assert!(field_names.contains(&"country"));
    }
}
