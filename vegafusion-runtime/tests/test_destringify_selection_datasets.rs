fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
#[cfg(test)]
mod tests {
    use crate::crate_dir;
    use std::fs;
    use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
    use vegafusion_core::runtime::VegaFusionRuntimeTrait;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::spec::transform::TransformSpec;
    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

    #[tokio::test]
    async fn test_destringify_selection_datasets() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/datetime_strings_in_selection_stores.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);

        let (chart_spec, _warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    keep_variables: vec![],
                    row_limit: None,
                    local_tz: "UTC".to_string(),
                    default_input_tz: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        let click_store = &chart_spec.data[2];
        assert_eq!(click_store.name.as_str(), "click_store");
        assert_eq!(click_store.transform.len(), 1);
        if let TransformSpec::Formula(formula) = &click_store.transform[0] {
            assert_eq!(formula.expr, "[toDate(datum.values[0]), datum.values[1]]");
        } else {
            panic!("Unexpected transform")
        }

        let drag_store = &chart_spec.data[3];
        assert_eq!(drag_store.name.as_str(), "drag_store");
        assert_eq!(drag_store.transform.len(), 1);
        if let TransformSpec::Formula(formula) = &drag_store.transform[0] {
            assert_eq!(
                formula.expr,
                "[[toDate(datum.values[0][0]), toDate(datum.values[0][1])]]"
            );
        } else {
            panic!("Unexpected transform")
        }
    }
}
