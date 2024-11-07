fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
#[cfg(test)]
mod tests {
    use crate::crate_dir;

    use std::fs;
    use vegafusion_common::error::VegaFusionError;
    use vegafusion_core::proto::gen::pretransform::{PreTransformSpecOpts, PreTransformVariable};
    use vegafusion_core::proto::gen::tasks::Variable;
    use vegafusion_core::runtime::VegaFusionRuntimeTrait;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

    #[tokio::test]
    async fn test_pre_transform_keep_variables() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/manual_histogram.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);

        let (tx_spec, warnings) = runtime
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

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Without keep_variables, there should be no signals
        assert!(tx_spec.signals.is_empty());

        // Now rerun with keep_variables to keep the bin signal
        let (tx_spec, warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    keep_variables: vec![PreTransformVariable {
                        variable: Some(Variable::new_signal(
                            "layer_0_layer_0_bin_maxbins_10_IMDB_Rating_bins",
                        )),
                        scope: Vec::new(),
                    }],
                    row_limit: None,
                    local_tz: "UTC".to_string(),
                    default_input_tz: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        assert!(warnings.is_empty());
        assert_eq!(tx_spec.signals.len(), 1);

        let signal0 = tx_spec.signals.first().unwrap();
        assert_eq!(
            signal0.name,
            "layer_0_layer_0_bin_maxbins_10_IMDB_Rating_bins"
        );

        // Rerun with non-existent signal
        let pre_transform_result = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    keep_variables: vec![PreTransformVariable {
                        variable: Some(Variable::new_signal("does_not_exist")),
                        scope: Vec::new(),
                    }],
                    row_limit: None,
                    local_tz: "UTC".to_string(),
                    default_input_tz: None,
                    preserve_interactivity: true,
                },
            )
            .await;

        if !matches!(
            pre_transform_result,
            Err(VegaFusionError::PreTransformError(_, _))
        ) {
            panic!("Expected error when keep variable does not exist")
        }
    }
}
