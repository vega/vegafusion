#[macro_use]
extern crate lazy_static;
mod util;

#[cfg(test)]
mod test_fold {
    use crate::util::check::check_transform_evaluation;
    use rstest::rstest;
    use serde_json::json;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_core::spec::transform::TransformSpec;

    fn simple_medals_dataset() -> VegaFusionTable {
        VegaFusionTable::from_json(&json!([
          {"country": "USA", "gold": 10, "silver": 20},
          {"country": "Canada", "gold": 7, "silver": 26}
        ]))
        .unwrap()
    }

    #[rstest(
        fields,
        case(vec!["gold"]),
        case(vec!["silver"]),
        case(vec!["gold", "silver"]),
        case(vec!["silver", "gold"]),
        case(vec!["silver", "gold", "bogus"]),
        case(vec!["gold", "bogus", "silver"]),
    )]
    fn test(fields: Vec<&str>) {
        let dataset = simple_medals_dataset();
        let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
            {"type": "fold", "fields": fields, "as": ["key", "value"]}
        ]))
        .unwrap();

        let comp_config = Default::default();
        let eq_config = Default::default();

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
