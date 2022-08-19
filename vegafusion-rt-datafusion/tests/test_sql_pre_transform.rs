/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
#[macro_use]
extern crate lazy_static;

mod util;

#[cfg(test)]
mod tests {
    use crate::util::crate_dir;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use vegafusion_core::error::VegaFusionError;
    use vegafusion_core::proto::gen::tasks::Variable;
    use vegafusion_rt_datafusion::data::dataset::VegaFusionDataset;
    use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
    use vegafusion_rt_datafusion::sql::connection::sqlite_conn::SqLiteConnection;
    use vegafusion_rt_datafusion::sql::dataframe::SqlDataFrame;
    use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

    #[tokio::test]
    async fn test_sql_pre_transform_histogram() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/inline_datasets/histogram.vg.json",
            // "{}/tests/specs/inline_datasets/rect_binned_heatmap.vg.json",
            // "{}/tests/specs/inline_datasets/layer_histogram_global_mean.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();

        // Build sqlite-backed SqlDataFrame for movies dataset
        let conn =
            SqLiteConnection::try_new(&format!("{}/tests/data/vega_datasets.db", crate_dir()))
                .await
                .unwrap();

        let sql_df = SqlDataFrame::try_new(Arc::new(conn), "movie")
            .await
            .unwrap();

        // Build inline datasets
        let inline_datasets: HashMap<String, VegaFusionDataset> = vec![(
            "movie".to_string(),
            VegaFusionDataset::SqlDataFrame(Arc::new(sql_df)),
        )]
        .into_iter()
        .collect();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

        // Perform per transform values
        let (values, warnings) = runtime
            .pre_transform_values(
                &spec_str,
                &[(Variable::new_data("source_0"), vec![])],
                "UTC",
                &None,
                inline_datasets,
            )
            .await
            .unwrap();

        // Extract pre-transformed table
        let dataset = values[0].as_table().cloned().unwrap();

        println!("{}", dataset.pretty_format(None).unwrap());
    }
}
