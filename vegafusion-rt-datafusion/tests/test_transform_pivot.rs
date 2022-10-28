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
        case(Some(AggregateOpSpec::Min), Some(0))
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
        case(Some(AggregateOpSpec::Min), Some(0))
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
