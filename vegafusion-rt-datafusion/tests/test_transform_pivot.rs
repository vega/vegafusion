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
use serde_json::json;
use vegafusion_core::data::table::VegaFusionTable;

fn medals() -> VegaFusionTable {
    VegaFusionTable::from_json(
        &json!([
            {"country": "Germany", "type": "gold", "count": 14},
            {"country": "Norway", "type": "gold", "count": 14},
            {"country": "Norway", "type": "silver", "count": 14},
            {"country": "Canada", "type": "copper", "count": 10},
            {"country": "Norway", "type": "bronze", "count": 11},
            {"country": "Germany", "type": "silver", "count": 10},
            {"country": "Germany", "type": "bronze", "count": 7},
            {"country": "Canada", "type": "gold", "count": 11},
            {"country": "Canada", "type": "silver", "count": 8},
            {"country": "Canada", "type": "bronze", "count": 10},
        ]),
        1024,
    )
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
        case(None),
        case(Some(AggregateOpSpec::Sum)),
        case(Some(AggregateOpSpec::Count)),
        case(Some(AggregateOpSpec::Mean)),
        case(Some(AggregateOpSpec::Max)),
        case(Some(AggregateOpSpec::Min))
    )]
    fn test(op: Option<AggregateOpSpec>) {
        let dataset = medals();

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: Some(vec!["country".to_string()]),
            limit: None,
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
        case(None),
        case(Some(AggregateOpSpec::Sum)),
        case(Some(AggregateOpSpec::Count)),
        case(Some(AggregateOpSpec::Mean)),
        case(Some(AggregateOpSpec::Max)),
        case(Some(AggregateOpSpec::Min))
    )]
    fn test(op: Option<AggregateOpSpec>) {
        let dataset = medals();

        let pivot_spec = PivotTransformSpec {
            field: "type".to_string(),
            value: "count".to_string(),
            groupby: None,
            limit: None,
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
