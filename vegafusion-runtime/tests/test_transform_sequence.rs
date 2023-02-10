#[macro_use]
extern crate lazy_static;
mod util;

#[cfg(test)]
mod test_sequence {
    use crate::util::check::check_transform_evaluation;
    use rstest::rstest;
    use serde_json::json;
    use std::sync::Arc;
    use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
    use vegafusion_common::arrow::record_batch::RecordBatch;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest(
        start, stop, step, as_,
        case(json!(1), json!(10), None, None),
        case(json!(1), json!(10), Some(json!(3.14)), Some("pies")),
        case(json!(100), json!(10), Some(json!({"signal": "-4*5"})), None),
        case(json!({"signal": "5 - 7"}), json!({"signal": "17"}), Some(json!({"signal": "0.94"})), Some("another")),

        // Vega doesn't seem to match the spec here. It doesn't infer a step of -1
        // but returns an empty table
        // case(json!(5), json!(-10), None, Some("custom")),
    )]
    fn test(
        start: serde_json::Value,
        stop: serde_json::Value,
        step: Option<serde_json::Value>,
        as_: Option<&str>,
    ) {
        let empty_record_batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "_empty",
            DataType::Float64,
            true,
        )])));
        let empty_table = VegaFusionTable::from(empty_record_batch);

        let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
            {
                "type": "sequence",
                "start": start,
                "stop": stop,
                "step": step,
                "as": as_
            }
        ]))
        .unwrap();

        let comp_config = Default::default();
        let eq_config = Default::default();

        check_transform_evaluation(
            &empty_table,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
