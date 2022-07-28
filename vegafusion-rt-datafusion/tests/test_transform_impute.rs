#[macro_use]
extern crate lazy_static;

mod util;
use util::check::check_transform_evaluation;

#[cfg(test)]
mod test_impute {
    use crate::check_transform_evaluation;
    use crate::util::equality::TablesEqualConfig;
    use serde_json::json;
    use vegafusion_core::data::table::VegaFusionTable;
    use vegafusion_core::spec::transform::impute::{ImputeMethodSpec, ImputeTransformSpec};
    use vegafusion_core::spec::transform::TransformSpec;
    use vegafusion_core::spec::values::Field;

    fn simple_dataset() -> VegaFusionTable {
        VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": 28, "c": 0, "d": -1},
                {"a": 0, "b": 91, "c": 1, "d": -2},
                {"a": 1, "b": 43, "c": 0, "d": -3},
                {"a": 1, "b": 55, "c": 1, "d": -4},
                {"a": 3, "b": 19, "c": 0, "d": -7},
                {"a": 2, "b": 81, "c": 0, "d": -5},
                {"a": 2, "b": 53, "c": 1, "d": -6},

            ]),
            1024,
        )
        .unwrap()
    }

    #[test]
    fn test_one_groupby() {
        let dataset = simple_dataset();

        let impute_spec = ImputeTransformSpec {
            field: Field::String("b".to_string()),
            key: Field::String("a".to_string()),
            keyvals: None,
            method: Some(ImputeMethodSpec::Value),
            groupby: Some(vec![Field::String("c".to_string())]),
            value: Some(json!(0.0)),
            extra: Default::default(),
        };

        let transform_specs = vec![TransformSpec::Impute(impute_spec)];

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
    fn test_zero_groupby() {
        let dataset = simple_dataset();

        let impute_spec = ImputeTransformSpec {
            field: Field::String("b".to_string()),
            key: Field::String("a".to_string()),
            keyvals: None,
            method: Some(ImputeMethodSpec::Value),
            groupby: None,
            value: Some(json!(0.0)),
            extra: Default::default(),
        };

        let transform_specs = vec![TransformSpec::Impute(impute_spec)];

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
