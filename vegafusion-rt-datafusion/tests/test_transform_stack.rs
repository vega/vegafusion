/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
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
mod test_stack {
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
