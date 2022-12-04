#[cfg(test)]
mod tests {
    use crate::crate_dir;
    use std::fs;
    use vegafusion_core::proto::gen::services::pre_transform_spec_result;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::spec::transform::formula::FormulaTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;
    use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

    #[tokio::test]
    async fn test_destringify_selection_datasets() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/datetime_strings_in_selection_stores.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

        let result = runtime
            .pre_transform_spec(&spec_str, "UTC", &None, None, Default::default())
            .await
            .unwrap();

        match result.result.unwrap() {
            pre_transform_spec_result::Result::Response(response) => {
                let chart_spec: ChartSpec = serde_json::from_str(&response.spec).unwrap();

                let data1 = &chart_spec.data[1];
                assert_eq!(data1.name.as_str(), "click_store");
                assert_eq!(data1.transform.len(), 1);
                if let TransformSpec::Formula(formula) = &data1.transform[0] {
                    assert_eq!(formula.expr, "[toDate(datum.values[0]), datum.values[1]]");
                } else {
                    panic!("Unexpected transform")
                }

                let data2 = &chart_spec.data[2];
                assert_eq!(data2.name.as_str(), "drag_store");
                assert_eq!(data2.transform.len(), 1);
                if let TransformSpec::Formula(formula) = &data2.transform[0] {
                    assert_eq!(
                        formula.expr,
                        "[[toDate(datum.values[0][0]), toDate(datum.values[0][1])]]"
                    );
                } else {
                    panic!("Unexpected transform")
                }
            }
            pre_transform_spec_result::Result::Error(err) => {
                panic!("Unexpected pre_transform_spec error: {:?}", err);
            }
        }
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
