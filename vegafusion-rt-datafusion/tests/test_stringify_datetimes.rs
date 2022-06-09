use lazy_static::lazy_static;
use tokio::runtime::Runtime;

lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

#[cfg(test)]
mod test_stringify_datetimes {
    use crate::{crate_dir, TOKIO_RUNTIME};
    use rstest::rstest;
    use std::fs;
    use vegafusion_core::proto::gen::pretransform::pre_transform_warning::WarningType;
    use vegafusion_core::proto::gen::services::pre_transform_result;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

    #[rstest(
        local_tz,
        default_input_tz,
        expected_hours_time,
        expected_hours_time_end,
        case("UTC", "UTC", "2012-01-01 01:00:00.000", "2012-01-01 02:00:00.000"),
        case(
            "America/New_York",
            "America/New_York",
            "2012-01-01 01:00:00.000",
            "2012-01-01 02:00:00.000"
        ),
        case(
            "UTC",
            "America/New_York",
            "2012-01-01 06:00:00.000",
            "2012-01-01 07:00:00.000"
        ),
        case(
            "America/Los_Angeles",
            "America/New_York",
            "2012-01-01 22:00:00.000",
            "2012-01-01 23:00:00.000"
        ),
        case(
            "America/New_York",
            "America/Los_Angeles",
            "2012-01-01 04:00:00.000",
            "2012-01-01 05:00:00.000"
        )
    )]
    fn test(
        local_tz: &str,
        default_input_tz: &str,
        expected_hours_time: &str,
        expected_hours_time_end: &str,
    ) {
        TOKIO_RUNTIME.block_on(check_github_hist(
            local_tz,
            default_input_tz,
            expected_hours_time,
            expected_hours_time_end,
        ));
    }

    async fn check_github_hist(
        local_tz: &str,
        default_input_tz: &str,
        expected_hours_time: &str,
        expected_hours_time_end: &str,
    ) {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/github_hist.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));
        let local_tz = local_tz.to_string();

        let pre_tx_result = runtime
            .pre_transform_spec(
                &spec_str,
                &local_tz,
                &Some(default_input_tz.to_string()),
                None,
                Default::default(),
            )
            .await
            .unwrap();

        match pre_tx_result.result.unwrap() {
            pre_transform_result::Result::Error(err) => {
                panic!("pre_transform_spec error: {:?}", err);
            }
            pre_transform_result::Result::Response(response) => {
                let spec: ChartSpec = serde_json::from_str(&response.spec).unwrap();
                let data = spec.data[0].values.as_ref().unwrap();
                let values = data.as_array().expect("Expected array");
                let first = values[0].as_object().expect("Expected object");
                println!("{:?}", first);

                // Check hours_time
                let hours_time = first
                    .get("hours_time")
                    .expect("Expected hours_time")
                    .as_str()
                    .expect("Expected string")
                    .to_string();
                assert_eq!(hours_time, expected_hours_time);

                // Check hours_time_end
                let hours_time_end = first
                    .get("hours_time_end")
                    .expect("Expected hours_time_end")
                    .as_str()
                    .expect("Expected string")
                    .to_string();
                assert_eq!(hours_time_end, expected_hours_time_end);
            }
        }
    }

    #[tokio::test]
    async fn test_mixed_scale_warning() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/shipping_mixed_scales.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));
        let local_tz = "UTC".to_string();

        let pre_tx_result = runtime
            .pre_transform_spec(&spec_str, &local_tz, &None, None, Default::default())
            .await
            .unwrap();

        let pre_tx_result = pre_tx_result.result.unwrap();

        match pre_tx_result {
            pre_transform_result::Result::Response(response) => {
                let warnings = response.warnings;
                assert_eq!(warnings.len(), 1);
                let warning = warnings.get(0).unwrap();
                if let WarningType::Planner(warning) = warning.warning_type.as_ref().unwrap() {
                    assert_eq!(
                        warning.message,
                        "Field \"ship_date\" of dataset \"data_0\" is scaled using both time and the following non-time scales: \"ordinal\""
                    )
                } else {
                    panic!(
                        "Expected Planner warning type, received: {:?}",
                        warning.warning_type
                    )
                }
            }
            pre_transform_result::Result::Error(err) => {
                panic!("Pre Transform Error: {:?}", err)
            }
        }
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
