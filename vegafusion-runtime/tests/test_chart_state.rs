#[cfg(test)]
mod tests {
    use crate::crate_dir;
    use serde_json::json;
    use std::fs;
    use std::sync::Arc;
    use vegafusion_core::planning::watch::{ExportUpdateJSON, ExportUpdateNamespace};
    use vegafusion_core::proto::gen::tasks::TzConfig;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_runtime::task_graph::runtime::{ChartState, VegaFusionRuntime};
    use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

    #[tokio::test]
    async fn test_chart_state() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/custom/flights_crossfilter_a.vg.json",
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

        let chart_state = ChartState::try_new(
            &runtime,
            spec,
            Default::default(),
            TzConfig {
                local_tz: "UTC".to_string(),
                default_input_tz: None,
            },
            None,
        )
        .await
        .unwrap();

        let updates = vec![ExportUpdateJSON {
            namespace: ExportUpdateNamespace::Data,
            name: "brush_store".to_string(),
            scope: vec![],
            value: json!([
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
                    1100,
                    1500
                  ]
                ]
              }
            ]),
        }];

        let response_updates = chart_state.update(&runtime, updates).await.unwrap();
        let expected_updates: Vec<ExportUpdateJSON> = serde_json::from_value(json!(
        [
          {
            "namespace": "data",
            "name": "data_1",
            "scope": [],
            "value": [
              {
                "__count": 44,
                "bin_maxbins_20_delay": -20.0,
                "bin_maxbins_20_delay_end": 0.0
              },
              {
                "__count": 27,
                "bin_maxbins_20_delay": 0.0,
                "bin_maxbins_20_delay_end": 20.0
              },
              {
                "__count": 8,
                "bin_maxbins_20_delay": 20.0,
                "bin_maxbins_20_delay_end": 40.0
              },
              {
                "__count": 5,
                "bin_maxbins_20_delay": -40.0,
                "bin_maxbins_20_delay_end": -20.0
              },
              {
                "__count": 1,
                "bin_maxbins_20_delay": 40.0,
                "bin_maxbins_20_delay_end": 60.0
              },
              {
                "__count": 2,
                "bin_maxbins_20_delay": 60.0,
                "bin_maxbins_20_delay_end": 80.0
              }
            ]
          },
          {
            "namespace": "data",
            "name": "data_2",
            "scope": [],
            "value": [
              {
                "__count": 23,
                "bin_maxbins_20_distance": 1400.0,
                "bin_maxbins_20_distance_end": 1600.0
              },
              {
                "__count": 30,
                "bin_maxbins_20_distance": 1000.0,
                "bin_maxbins_20_distance_end": 1200.0
              },
              {
                "__count": 34,
                "bin_maxbins_20_distance": 1200.0,
                "bin_maxbins_20_distance_end": 1400.0
              }
            ]
          }
        ]
        ))
        .unwrap();

        // println!(
        //     "{}",
        //     serde_json::to_string_pretty(&response_updates).unwrap()
        // );

        assert_eq!(response_updates, expected_updates)
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
