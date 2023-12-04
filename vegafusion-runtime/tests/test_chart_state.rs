#[cfg(test)]
mod tests {
    use crate::{crate_dir};
    use serde_json::json;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::sync::Arc;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_common::error::VegaFusionError;
    use vegafusion_core::planning::watch::{ExportUpdateJSON, ExportUpdateNamespace};
    use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType;
    use vegafusion_core::proto::gen::tasks::{TzConfig, Variable};
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::spec::values::StringOrSignalSpec;
    use vegafusion_runtime::data::dataset::VegaFusionDataset;
    use vegafusion_runtime::task_graph::runtime::{ChartState, VegaFusionRuntime};
    use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

    #[tokio::test]
    async fn test_pre_transform_dataset() {
        // Load spec
        let spec_path = format!("{}/tests/specs/custom/flights_crossfilter_a.vg.json", crate_dir());
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(
            Arc::new(DataFusionConnection::default()),
            Some(16),
            Some(1024_i32.pow(3) as usize),
        );

        let chart_state = ChartState::try_new(
            &runtime,
            spec,
            Default::default(),
            TzConfig { local_tz: "UTC".to_string(), default_input_tz: None },
            None
        ).await.unwrap();

        let updates = vec![ExportUpdateJSON {
            namespace: ExportUpdateNamespace::Data,
            name: "brush_store".to_string(),
            scope: vec![],
            value:json!([
              {
                "unit": "child__column_distance_layer_0",
                "fields": [
                  {
                    "field": "distance",
                    "channel": "x",
                    "type": "R"
                  }
                ],
                "values": [
                  [
                    1161.5625,
                    1449.5625
                  ]
                ]
              }
            ]),
        }];

        let response_updates = chart_state.update(&runtime, updates).await.unwrap();
        println!("{response_updates:?}");

    }
}


fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
