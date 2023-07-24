#[cfg(test)]
mod tests {
    use crate::crate_dir;
    use serde_json::json;

    use std::fs;
    use std::sync::Arc;

    use vegafusion_core::spec::chart::ChartSpec;

    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;
    use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

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
        let runtime = VegaFusionRuntime::new(
            Arc::new(DataFusionConnection::default()),
            Some(16),
            Some(1024_i32.pow(3) as usize),
        );

        let (tx_spec, datasets, warnings) = runtime
            .pre_transform_extract(
                &spec,
                "UTC",
                &None,
                true,
                20,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Check single extracted dataset
        assert_eq!(datasets.len(), 1);
        let dataset = datasets[0].clone();
        assert_eq!(dataset.0.as_str(), "source_0");
        assert_eq!(dataset.1, Vec::<u32>::new());
        assert_eq!(dataset.2.num_rows(), 379);

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

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
