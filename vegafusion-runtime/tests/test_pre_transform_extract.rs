fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
#[cfg(test)]
mod tests {
    use crate::crate_dir;
    use serde_json::json;
    use vegafusion_core::proto::gen::pretransform::PreTransformExtractOpts;

    use std::fs;

    use vegafusion_core::runtime::VegaFusionRuntimeTrait;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

    #[tokio::test]
    async fn test_pre_transform_extract_scatter() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/vegalite/rect_binned_heatmap.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);

        let (tx_spec, datasets, warnings) = runtime
            .pre_transform_extract(
                &spec,
                &Default::default(),
                &PreTransformExtractOpts {
                    keep_variables: vec![],
                    extract_threshold: 20,
                    preserve_interactivity: false,
                    local_tz: "UTC".to_string(),
                    default_input_tz: None,
                },
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Check single extracted dataset
        assert_eq!(datasets.len(), 1);
        let dataset = &datasets[0];
        assert_eq!(dataset.name.as_str(), "source_0");
        assert_eq!(dataset.scope, Vec::<u32>::new());
        assert_eq!(dataset.table.num_rows(), 379);

        // Check that source_0 is included as a stub in the transformed spec
        let source_0 = &tx_spec.data[0];
        assert_eq!(source_0.name.as_str(), "source_0");
        assert_eq!(source_0.values, None);

        // Check that source_0_color_domain___count was included inline (since it's small)
        let source_0_color_domain_count = &tx_spec.data[1];
        assert_eq!(
            source_0_color_domain_count.name.as_str(),
            "source_0_color_domain___count"
        );
        assert_eq!(
            source_0_color_domain_count.values,
            Some(json!([{"min": 1, "max": 24}]))
        );
    }
}
