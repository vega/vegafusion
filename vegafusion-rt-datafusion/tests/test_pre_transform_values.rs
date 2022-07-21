/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
#[cfg(test)]
mod tests {
    use crate::crate_dir;
    use std::fs;
    use vegafusion_core::error::VegaFusionError;
    use vegafusion_core::proto::gen::tasks::Variable;
    use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
    use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

    #[tokio::test]
    async fn test_pre_transform_dataset() {
        // Load spec
        let spec_path = format!("{}/tests/specs/vegalite/histogram.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

        let (values, warnings) = runtime
            .pre_transform_values(
                &spec_str,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                Default::default(),
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Check single returned dataset
        assert_eq!(values.len(), 1);

        let dataset = values[0].as_table().cloned().unwrap();

        let expected = "\
+----------------------------+--------------------------------+---------+
| bin_maxbins_10_IMDB Rating | bin_maxbins_10_IMDB Rating_end | __count |
+----------------------------+--------------------------------+---------+
| 6                          | 7                              | 985     |
| 3                          | 4                              | 100     |
| 7                          | 8                              | 741     |
| 5                          | 6                              | 633     |
| 8                          | 9                              | 204     |
| 2                          | 3                              | 43      |
| 4                          | 5                              | 273     |
| 9                          | 10                             | 4       |
| 1                          | 2                              | 5       |
+----------------------------+--------------------------------+---------+";
        assert_eq!(dataset.pretty_format(None).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_pre_transform_validate() {
        // Load spec
        let spec_path = format!("{}/tests/specs/vegalite/area_density.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

        // Check existent but unsupported dataset name
        let result = runtime
            .pre_transform_values(
                &spec_str,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                Default::default(),
            )
            .await;

        if let Err(VegaFusionError::PreTransformError(err, _)) = result {
            assert_eq!(
                err,
                "Requested variable (Variable { name: \"source_0\", namespace: Data }, [])\n \
                requires transforms or signal expressions that are not yet supported"
            )
        } else {
            panic!("Expected PreTransformError");
        }

        // Check non-existent dataset name
        let result = runtime
            .pre_transform_values(
                &spec_str,
                &[(Variable::new_data("bogus_0"), vec![])],
                "UTC",
                &None,
                Default::default(),
            )
            .await;

        if let Err(VegaFusionError::PreTransformError(err, _)) = result {
            assert_eq!(err, "No dataset named bogus_0 with scope []")
        } else {
            panic!("Expected PreTransformError");
        }
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
