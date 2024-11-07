use lazy_static::lazy_static;
use tokio::runtime::Runtime;
use vegafusion_runtime::tokio_runtime::TOKIO_THREAD_STACK_SIZE;

lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
        .build()
        .unwrap();
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

#[cfg(test)]
mod test_stringify_datetimes {
    use crate::{crate_dir, TOKIO_RUNTIME};
    use rstest::rstest;
    use std::fs;
    use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
    use vegafusion_core::runtime::VegaFusionRuntimeTrait;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

    #[rstest(
        local_tz,
        default_input_tz,
        expected_hours_time,
        expected_hours_time_end,
        case("UTC", "UTC", "2012-01-01T01:00:00.000", "2012-01-01T02:00:00.000"),
        case(
            "America/New_York",
            "America/New_York",
            "2012-01-01T01:00:00.000",
            "2012-01-01T02:00:00.000"
        ),
        case(
            "UTC",
            "America/New_York",
            "2012-01-01T06:00:00.000",
            "2012-01-01T07:00:00.000"
        ),
        case(
            "America/Los_Angeles",
            "America/New_York",
            "2012-01-01T22:00:00.000",
            "2012-01-01T23:00:00.000"
        ),
        case(
            "America/New_York",
            "America/Los_Angeles",
            "2012-01-01T04:00:00.000",
            "2012-01-01T05:00:00.000"
        )
    )]
    fn test_github_hist(
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
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);
        let local_tz = local_tz.to_string();

        let (spec, _warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    local_tz: local_tz.to_string(),
                    default_input_tz: Some(default_input_tz.to_string()),
                    keep_variables: vec![],
                    row_limit: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        let data = spec.data[0].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");
        println!("{first:?}");

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

    #[tokio::test]
    async fn test_timeunit_ordinal() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/timeunit_ordinal.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);
        // let local_tz = "America/New_York".to_string();
        let local_tz = "UTC".to_string();
        let default_input_tz = "UTC".to_string();

        let (spec, _warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    local_tz: local_tz.to_string(),
                    default_input_tz: Some(default_input_tz.to_string()),
                    keep_variables: vec![],
                    row_limit: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        println!("{}", serde_json::to_string_pretty(&spec).unwrap());

        assert_eq!(&spec.data[0].name, "source_0");
        let data = spec.data[0].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");

        // Check hours_time
        let expected_month_date = "2012-01-01T00:00:00.000";
        let hours_time = first
            .get("month_date")
            .expect("Expected month_date")
            .as_str()
            .expect("Expected month_date value to be a string")
            .to_string();
        assert_eq!(hours_time, expected_month_date);

        // Check domain includes string datetimes
        assert_eq!(&spec.data[1].name, "source_0_x_domain_month_date");
        let data = spec.data[1].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");

        let expected_month_date = "2012-01-01T00:00:00.000";
        let hours_time = first
            .get("month_date")
            .expect("Expected month_date")
            .as_str()
            .expect("Expected month_date value to be a string")
            .to_string();
        assert_eq!(hours_time, expected_month_date);
    }

    #[rstest(
        local_tz,
        default_input_tz,
        expected_ship_date,
        case("UTC", "UTC", "2011-01-08T00:00:00.000"),
        case("America/New_York", "UTC", "2011-01-07T19:00:00.000"),
        case("UTC", "America/New_York", "2011-01-08T00:00:00.000")
    )]
    fn test_local_datetime_ordinal_color(
        local_tz: &str,
        default_input_tz: &str,
        expected_ship_date: &str,
    ) {
        TOKIO_RUNTIME.block_on(check_local_datetime_ordinal_color(
            local_tz,
            default_input_tz,
            expected_ship_date,
        ));
    }

    async fn check_local_datetime_ordinal_color(
        local_tz: &str,
        default_input_tz: &str,
        expected_ship_date: &str,
    ) {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/shipping_mixed_scales.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();
        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);

        let (spec, _warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    local_tz: local_tz.to_string(),
                    default_input_tz: Some(default_input_tz.to_string()),
                    keep_variables: vec![],
                    row_limit: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        println!("{}", serde_json::to_string_pretty(&spec).unwrap());

        assert_eq!(&spec.data[1].name, "data_0");
        let data = spec.data[1].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");

        // Check hours_time
        let hours_time = first
            .get("ship_date")
            .expect("Expected ship_date")
            .as_str()
            .expect("Expected ship_date value to be a string")
            .to_string();
        assert_eq!(hours_time, expected_ship_date);

        // Check domain includes string datetimes
        assert_eq!(&spec.data[3].name, "data_0_color_domain_ship_date");
        let data = spec.data[3].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");

        let hours_time = first
            .get("ship_date")
            .expect("Expected ship_date")
            .as_str()
            .expect("Expected ship_date value to be a string")
            .to_string();
        assert_eq!(hours_time, expected_ship_date);
    }

    /// This test checks that we're able to identify local timestamp column usage inside facet
    /// definitions
    #[test]
    fn test_local_datetime_facet_usage() {
        let local_tz = "UTC";
        let default_input_tz = "America/New_York";
        let expected_year_date = "2000-01-01T00:00:00.000";

        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/stocks_layered_pivot_bug.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);

        let (spec, _warnings) = TOKIO_RUNTIME
            .block_on(runtime.pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    local_tz: local_tz.to_string(),
                    default_input_tz: Some(default_input_tz.to_string()),
                    keep_variables: vec![],
                    row_limit: None,
                    preserve_interactivity: true,
                },
            ))
            .unwrap();

        println!("{}", serde_json::to_string_pretty(&spec).unwrap());

        assert_eq!(&spec.data[7].name, "data_5");

        let data = spec.data[7].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");

        // Check year_date
        let hours_time = first
            .get("year_date")
            .expect("Expected year_date column")
            .as_str()
            .expect("Expected year_date value to be a string")
            .to_string();
        assert_eq!(hours_time, expected_year_date);
    }

    #[tokio::test]
    async fn test_pre_transform_stringify_datetimes_in_upstream_dataset() {
        // Load spec
        let spec_path = format!(
            "{}/tests/specs/pre_transform/interactive_average.vg.json",
            crate_dir()
        );
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Initialize task graph runtime
        let runtime = VegaFusionRuntime::new(None);

        let (spec, _warnings) = runtime
            .pre_transform_spec(
                &spec,
                &Default::default(),
                &PreTransformSpecOpts {
                    local_tz: "UTC".to_string(),
                    default_input_tz: None,
                    keep_variables: vec![],
                    row_limit: None,
                    preserve_interactivity: true,
                },
            )
            .await
            .unwrap();

        println!("{}", serde_json::to_string_pretty(&spec).unwrap());

        assert_eq!(&spec.data[1].name, "source_0");

        let data = spec.data[1].values.as_ref().unwrap();
        let values = data.as_array().expect("Expected array");
        let first = values[0].as_object().expect("Expected object");

        // Check year_date
        let month_date = first
            .get("month_date")
            .expect("Expected month_date column")
            .as_str()
            .expect("Expected month_date value to be a string")
            .to_string();
        assert_eq!(month_date, "2012-01-01T00:00:00.000");
    }
}
