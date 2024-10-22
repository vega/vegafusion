#[macro_use]
extern crate lazy_static;

mod util;

use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use util::equality::TablesEqualConfig;
use vegafusion_core::spec::transform::formula::FormulaTransformSpec;

use rstest::rstest;
use vegafusion_core::spec::transform::stack::StackTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::spec::values::{
    CompareSpec, Field, SortOrderOrList, SortOrderSpec, StringOrStringList,
};

#[cfg(test)]
mod test_stack_no_group {
    use super::*;
    use vegafusion_core::spec::transform::stack::StackOffsetSpec;

    #[rstest(
        offset,
        case(None),
        case(Some(StackOffsetSpec::Zero)),
        case(Some(StackOffsetSpec::Normalize)),
        case(Some(StackOffsetSpec::Center))
    )]
    fn test(offset: Option<StackOffsetSpec>) {
        let dataset = vega_json_dataset("penguins");

        let stack_spec = StackTransformSpec {
            field: Field::String("Body Mass (g)".to_string()),
            groupby: None,
            sort: None,
            as_: None,
            offset,
            extra: Default::default(),
        };

        println!("{}", serde_json::to_string_pretty(&stack_spec).unwrap());

        let transform_specs = vec![TransformSpec::Stack(stack_spec)];

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

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_stack_with_group {
    use super::*;
    use vegafusion_core::spec::transform::stack::StackOffsetSpec;

    #[rstest(
        offset,
        case(None),
        case(Some(StackOffsetSpec::Zero)),
        case(Some(StackOffsetSpec::Normalize)),
        case(Some(StackOffsetSpec::Center))
    )]
    fn test(offset: Option<StackOffsetSpec>) {
        let dataset = vega_json_dataset("penguins");

        let stack_spec = StackTransformSpec {
            field: Field::String("Body Mass (g)".to_string()),
            groupby: Some(vec![Field::String("Species".to_string())]),
            sort: None,
            as_: None,
            offset,
            extra: Default::default(),
        };

        let transform_specs = vec![TransformSpec::Stack(stack_spec)];

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

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_stack_with_group_sort {
    use super::*;
    use vegafusion_core::spec::transform::stack::StackOffsetSpec;

    #[rstest(
        offset,
        case(None),
        case(Some(StackOffsetSpec::Zero)),
        case(Some(StackOffsetSpec::Normalize)),
        case(Some(StackOffsetSpec::Center))
    )]
    fn test(offset: Option<StackOffsetSpec>) {
        let dataset = vega_json_dataset("penguins");

        let stack_spec = StackTransformSpec {
            field: Field::String("Body Mass (g)".to_string()),
            groupby: Some(vec![Field::String("Species".to_string())]),
            sort: Some(CompareSpec {
                field: StringOrStringList::StringList(vec![
                    "Sex".to_string(),
                    "Flipper Length (mm)".to_string(),
                ]),
                order: Some(SortOrderOrList::SortOrderList(vec![
                    SortOrderSpec::Ascending,
                    SortOrderSpec::Descending,
                ])),
            }),
            as_: Some(vec!["foo".to_string(), "bar".to_string()]),
            offset,
            extra: Default::default(),
        };

        let transform_specs = vec![TransformSpec::Stack(stack_spec)];

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

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_stack_with_group_sort_negative {
    use super::*;
    use vegafusion_core::spec::transform::stack::StackOffsetSpec;

    #[rstest(
        offset,
        case(None),
        case(Some(StackOffsetSpec::Zero)),
        case(Some(StackOffsetSpec::Normalize)),
        case(Some(StackOffsetSpec::Center))
    )]
    fn test(offset: Option<StackOffsetSpec>) {
        let dataset = vega_json_dataset("penguins");

        let formula_spec = FormulaTransformSpec {
            expr: "(datum['Body Mass (g)'] || 0) - 4000".to_string(),
            as_: "Body Mass (g)".to_string(),
            extra: Default::default(),
        };

        let stack_spec = StackTransformSpec {
            field: Field::String("Body Mass (g)".to_string()),
            groupby: Some(vec![Field::String("Species".to_string())]),
            sort: Some(CompareSpec {
                field: StringOrStringList::StringList(vec![
                    "Sex".to_string(),
                    "Flipper Length (mm)".to_string(),
                ]),
                order: Some(SortOrderOrList::SortOrderList(vec![
                    SortOrderSpec::Ascending,
                    SortOrderSpec::Descending,
                ])),
            }),
            as_: Some(vec!["foo".to_string(), "bar".to_string()]),
            offset,
            extra: Default::default(),
        };

        let transform_specs = vec![
            TransformSpec::Formula(formula_spec),
            TransformSpec::Stack(stack_spec),
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

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_stack_no_divide_by_zero {
    use super::*;
    use serde_json::json;
    use vegafusion_core::spec::transform::collect::CollectTransformSpec;
    use vegafusion_core::spec::transform::timeunit::TimeUnitTransformSpec;

    #[test]
    fn test() {
        let dataset = vega_json_dataset("movies");
        let formula_spec: FormulaTransformSpec = serde_json::from_value(json!(
            {
                "expr": "toDate(datum['Release Date'])",
                "as": "Release Date"
            }
        ))
        .unwrap();

        let timeunit_spec: TimeUnitTransformSpec = serde_json::from_value(json!(
            {
              "field": "Release Date",
              "type": "timeunit",
              "units": ["year"],
              "as": ["year_Release Date", "year_Release Date_end"]
            }
        ))
        .unwrap();

        let stack_spec: StackTransformSpec = serde_json::from_value(json!(
            {
              "type": "stack",
              "groupby": ["year_Release Date"],
              "field": "US Gross",
              "sort": {"field": ["Release Date", "Title"], "order": ["descending", "descending"]},
              "as": ["US Gross_start", "US Gross_end"],
              "offset": "normalize"
            }
        ))
        .unwrap();

        let collect_spec: CollectTransformSpec = serde_json::from_value(json!(
            {
              "type": "collect",
              "sort": {"field": ["Title", "Release Date"]}
            }
        ))
        .unwrap();

        // Vega sometimes produces NULL or NaN when a stack group has zero sum.
        // Replace these with 0 to match VegaFusion's behavior
        let start_formula_spec: FormulaTransformSpec = serde_json::from_value(json!(
            {
                "expr": "if(isValid(datum['US Gross_start']) && isFinite(datum['US Gross_start']), datum['US Gross_start'], 0)",
                "as": "US Gross_start"
            }
        )).unwrap();

        let end_formula_spec: FormulaTransformSpec = serde_json::from_value(json!(
            {
                "expr": "if(isValid(datum['US Gross_end']) && isFinite(datum['US Gross_end']), datum['US Gross_end'], 0)",
                "as": "US Gross_end"
            }
        )).unwrap();

        let transform_specs = vec![
            TransformSpec::Formula(formula_spec),
            TransformSpec::Timeunit(timeunit_spec),
            TransformSpec::Stack(stack_spec),
            TransformSpec::Collect(collect_spec),
            TransformSpec::Formula(start_formula_spec),
            TransformSpec::Formula(end_formula_spec),
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

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_stack_timestamp_group {
    use super::*;
    use serde_json::json;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_core::spec::transform::stack::StackOffsetSpec;

    #[rstest(
        offset,
        case(None),
        case(Some(StackOffsetSpec::Zero)),
        case(Some(StackOffsetSpec::Normalize)),
        case(Some(StackOffsetSpec::Center))
    )]
    fn test(offset: Option<StackOffsetSpec>) {
        let dataset = VegaFusionTable::from_json(&json!([
            {
              "RELEASE_DATE": "Jun 12 1998"
            },
            {
              "RELEASE_DATE": "Aug 07 1998"
            },
            {
              "RELEASE_DATE": "Aug 28 1998"
            },
            {
              "RELEASE_DATE": "Sep 11 1998"
            },
            {
              "RELEASE_DATE": "Oct 09 1998"
            }
        ]))
        .unwrap();

        let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
            {
              "type": "formula",
              "expr": "toDate(datum[\"RELEASE_DATE\"])",
              "as": "RELEASE_DATE"
            },
            {
              "type": "stack",
              "groupby": [
                "RELEASE_DATE"
              ],
              "field": "RELEASE_DATE",
              "sort": {
                "field": [],
                "order": []
              },
              "as": [
                "__RELEASE_DATE_start__",
                "__RELEASE_DATE_end__"
              ],
              "offset": offset
            }
        ]))
        .unwrap();

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &Default::default(),
            &Default::default(),
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
