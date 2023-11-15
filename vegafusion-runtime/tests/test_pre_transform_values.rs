#[cfg(test)]
mod tests {
    use crate::{crate_dir, setup_s3_environment_vars};
    use serde_json::json;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::sync::Arc;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_common::error::VegaFusionError;
    use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType;
    use vegafusion_core::proto::gen::tasks::Variable;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::spec::values::StringOrSignalSpec;
    use vegafusion_runtime::data::dataset::VegaFusionDataset;
    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;
    use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

    #[tokio::test]
    async fn test_pre_transform_dataset() {
        // Load spec
        let spec_path = format!("{}/tests/specs/vegalite/histogram.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(
            Arc::new(DataFusionConnection::default()),
            Some(16),
            Some(1024_i32.pow(3) as usize),
        );

        let (values, warnings) = runtime
            .pre_transform_values(
                &spec,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                None,
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
| 6.0                        | 7.0                            | 985     |
| 3.0                        | 4.0                            | 100     |
| 7.0                        | 8.0                            | 741     |
| 5.0                        | 6.0                            | 633     |
| 8.0                        | 9.0                            | 204     |
| 2.0                        | 3.0                            | 43      |
| 4.0                        | 5.0                            | 273     |
| 9.0                        | 10.0                           | 4       |
| 1.0                        | 2.0                            | 5       |
+----------------------------+--------------------------------+---------+";
        assert_eq!(dataset.pretty_format(None).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_pre_transform_dataset_with_row_limit() {
        // Load spec
        let spec_path = format!("{}/tests/specs/vegalite/histogram.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(
            Arc::new(DataFusionConnection::default()),
            Some(16),
            Some(1024_i32.pow(3) as usize),
        );

        let (values, warnings) = runtime
            .pre_transform_values(
                &spec,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                Some(3),
                Default::default(),
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert_eq!(warnings.len(), 1);
        if let Some(WarningType::RowLimit(row_limit)) = &warnings[0].warning_type {
            assert_eq!(row_limit.datasets.len(), 1);
            assert_eq!(row_limit.datasets[0].name, "source_0");
        } else {
            panic!("Unexpected warning type")
        }

        // Check single returned dataset
        assert_eq!(values.len(), 1);

        let dataset = values[0].as_table().cloned().unwrap();

        let expected = "\
+----------------------------+--------------------------------+---------+
| bin_maxbins_10_IMDB Rating | bin_maxbins_10_IMDB Rating_end | __count |
+----------------------------+--------------------------------+---------+
| 6.0                        | 7.0                            | 985     |
| 3.0                        | 4.0                            | 100     |
| 7.0                        | 8.0                            | 741     |
+----------------------------+--------------------------------+---------+";
        assert_eq!(dataset.pretty_format(None).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_pre_transform_validate() {
        // Load spec
        let spec_path = format!("{}/tests/specs/vegalite/area_density.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(
            Arc::new(DataFusionConnection::default()),
            Some(16),
            Some(1024_i32.pow(3) as usize),
        );

        // Check existent but unsupported dataset name
        let result = runtime
            .pre_transform_values(
                &spec,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                None,
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
                &spec,
                &[(Variable::new_data("bogus_0"), vec![])],
                "UTC",
                &None,
                None,
                Default::default(),
            )
            .await;

        if let Err(VegaFusionError::PreTransformError(err, _)) = result {
            assert_eq!(err, "No dataset named bogus_0 with scope []")
        } else {
            panic!("Expected PreTransformError");
        }
    }

    #[tokio::test]
    async fn test_pre_transform_with_dots_in_fieldname() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/inline_datasets/period_in_field_name.vg.json",
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

        let source_0 =
            VegaFusionTable::from_json(&json!([{"normal": 1, "a.b": 2}, {"normal": 1, "a.b": 4}]))
                .unwrap();

        let source_0_dataset =
            VegaFusionDataset::from_table_ipc_bytes(&source_0.to_ipc_bytes().unwrap()).unwrap();
        let inline_datasets: HashMap<_, _> = vec![("source_0".to_string(), source_0_dataset)]
            .into_iter()
            .collect();

        let (values, warnings) = runtime
            .pre_transform_values(
                &spec,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                None,
                inline_datasets,
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Check single returned dataset
        assert_eq!(values.len(), 1);

        let dataset = values[0].as_table().cloned().unwrap();
        println!("{}", dataset.pretty_format(None).unwrap());

        let expected = "\
+--------+-----+
| normal | a.b |
+--------+-----+
| 1      | 2   |
| 1      | 4   |
+--------+-----+";
        assert_eq!(dataset.pretty_format(None).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_pre_transform_with_empty_store() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/empty_store_array.vg.json",
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

        let (values, warnings) = runtime
            .pre_transform_values(
                &spec,
                &[(Variable::new_data("data_3"), vec![])],
                "UTC",
                &None,
                None,
                Default::default(),
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Check single returned dataset
        assert_eq!(values.len(), 1);

        let dataset = values[0].as_table().cloned().unwrap();
        let first_row = dataset.head(1);

        println!("{}", first_row.pretty_format(None).unwrap());

        let expected = "\
+-------+-----------+------+-----------------+
| yield | variety   | year | site            |
+-------+-----------+------+-----------------+
| 27.0  | Manchuria | 1931 | University Farm |
+-------+-----------+------+-----------------+";
        assert_eq!(first_row.pretty_format(None).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_pre_transform_with_datetime_strings_in_store() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/datetime_strings_in_selection_stores.vg.json",
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

        let (values, warnings) = runtime
            .pre_transform_values(
                &spec,
                &[
                    (Variable::new_data("click_selected"), vec![]),
                    (Variable::new_data("drag_selected"), vec![]),
                ],
                "UTC",
                &None,
                None,
                Default::default(),
            )
            .await
            .unwrap();

        // Check there are no warnings
        assert!(warnings.is_empty());

        // Check two returned datasets
        assert_eq!(values.len(), 2);

        // Check click_selected
        let click_selected = values[0].as_table().cloned().unwrap();

        println!("{}", click_selected.pretty_format(None).unwrap());

        let expected = "\
+---------------------+---------------------+---------+---------+---------------+-------------+
| yearmonth_date      | yearmonth_date_end  | weather | __count | __count_start | __count_end |
+---------------------+---------------------+---------+---------+---------------+-------------+
| 2013-11-01T00:00:00 | 2013-12-01T00:00:00 | rain    | 15      | 12.0          | 27.0        |
| 2014-01-01T00:00:00 | 2014-02-01T00:00:00 | sun     | 16      | 0.0           | 16.0        |
+---------------------+---------------------+---------+---------+---------------+-------------+";
        assert_eq!(click_selected.pretty_format(None).unwrap(), expected);

        // Check drag_selected
        let drag_selected = values[1].as_table().cloned().unwrap();
        println!("{}", drag_selected.pretty_format(None).unwrap());

        let expected = "\
+---------------------+---------------------+---------+---------+---------------+-------------+
| yearmonth_date      | yearmonth_date_end  | weather | __count | __count_start | __count_end |
+---------------------+---------------------+---------+---------+---------------+-------------+
| 2013-11-01T00:00:00 | 2013-12-01T00:00:00 | sun     | 12      | 0.0           | 12.0        |
| 2013-11-01T00:00:00 | 2013-12-01T00:00:00 | rain    | 15      | 12.0          | 27.0        |
| 2013-11-01T00:00:00 | 2013-12-01T00:00:00 | fog     | 2       | 27.0          | 29.0        |
| 2013-11-01T00:00:00 | 2013-12-01T00:00:00 | drizzle | 1       | 29.0          | 30.0        |
| 2013-12-01T00:00:00 | 2014-01-01T00:00:00 | sun     | 17      | 0.0           | 17.0        |
| 2013-12-01T00:00:00 | 2014-01-01T00:00:00 | snow    | 1       | 17.0          | 18.0        |
| 2013-12-01T00:00:00 | 2014-01-01T00:00:00 | rain    | 13      | 18.0          | 31.0        |
| 2014-01-01T00:00:00 | 2014-02-01T00:00:00 | sun     | 16      | 0.0           | 16.0        |
| 2014-01-01T00:00:00 | 2014-02-01T00:00:00 | rain    | 13      | 16.0          | 29.0        |
| 2014-01-01T00:00:00 | 2014-02-01T00:00:00 | fog     | 2       | 29.0          | 31.0        |
+---------------------+---------------------+---------+---------+---------------+-------------+";
        assert_eq!(drag_selected.pretty_format(None).unwrap(), expected);
    }

    #[tokio::test]
    async fn test_pre_transform_dataset_s3() {
        let run_s3_tests = env::var("VEGAFUSION_S3_TESTS").is_ok();
        if !run_s3_tests {
            return;
        }

        // Note: s3 tests require the pixi start-minio job
        setup_s3_environment_vars();

        // Load spec
        let spec_path = format!("{}/tests/specs/vegalite/histogram.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let mut spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        for file_type in ["json", "csv", "arrow", "parquet"] {
            // Prefix data/movies.json with s3://
            println!("File type: {file_type}");
            spec.data[0].url = Some(StringOrSignalSpec::String(format!(
                "s3://data/movies.{file_type}"
            )));

            // Initialize task graph runtime
            let runtime = VegaFusionRuntime::new(
                Arc::new(DataFusionConnection::default()),
                Some(16),
                Some(1024_i32.pow(3) as usize),
            );

            let (values, warnings) = runtime
                .pre_transform_values(
                    &spec,
                    &[(Variable::new_data("source_0"), vec![])],
                    "UTC",
                    &None,
                    None,
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
| 6.0                        | 7.0                            | 985     |
| 3.0                        | 4.0                            | 100     |
| 7.0                        | 8.0                            | 741     |
| 5.0                        | 6.0                            | 633     |
| 8.0                        | 9.0                            | 204     |
| 2.0                        | 3.0                            | 43      |
| 4.0                        | 5.0                            | 273     |
| 9.0                        | 10.0                           | 4       |
| 1.0                        | 2.0                            | 5       |
+----------------------------+--------------------------------+---------+";
            assert_eq!(dataset.pretty_format(None).unwrap(), expected);
        }
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

fn setup_s3_environment_vars() {
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    std::env::set_var("AWS_ACCESS_KEY_ID", "access_key123");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret_key123");
    std::env::set_var("AWS_ENDPOINT", "http://127.0.0.1:9000");
    std::env::set_var("AWS_ALLOW_HTTP", "true");
}
