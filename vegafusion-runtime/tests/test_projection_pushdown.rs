fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
#[cfg(test)]
mod test_custom_specs {
    use crate::crate_dir;
    use itertools::Itertools;
    use rstest::rstest;
    use std::fs;
    use vegafusion_core::planning::plan::{PlannerConfig, SpecPlan};
    use vegafusion_core::planning::projection_pushdown::get_column_usage;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    # [rstest(
        spec_name,
        data_index,
        projection_fields,
        case("vegalite/point_2d", 0, vec!["Horsepower", "Miles_per_Gallon"]),
        case("vegalite/point_bubble", 0, vec!["Acceleration", "Horsepower", "Miles_per_Gallon"]),
        case("vegalite/concat_marginal_histograms", 0, vec![
            "bin_maxbins_10_IMDB Rating",
            "bin_maxbins_10_IMDB Rating_end",
            "bin_maxbins_10_Rotten Tomatoes Rating",
            "bin_maxbins_10_Rotten Tomatoes Rating_end"
        ]),
        case("vegalite/rect_binned_heatmap", 0, vec![
            "__count",
            "bin_maxbins_40_Rotten Tomatoes Rating",
            "bin_maxbins_40_Rotten Tomatoes Rating_end",
            "bin_maxbins_60_IMDB Rating",
            "bin_maxbins_60_IMDB Rating_end"
        ]),
    )]
    fn test_proj_pushdown(spec_name: &str, data_index: usize, projection_fields: Vec<&str>) {
        // Load spec
        let spec_path = format!("{}/tests/specs/{}.vg.json", crate_dir(), spec_name);
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        let planner_config = PlannerConfig {
            projection_pushdown: true,
            fuse_datasets: false,
            ..Default::default()
        };
        let spec_plan = SpecPlan::try_new(&spec, &planner_config).unwrap();
        let data = &spec_plan.server_spec.data[data_index];
        let tx = &data.transform[data.transform.len() - 1];

        // Print data
        // println!("{}", serde_json::to_string_pretty(&spec_plan.server_spec.data).unwrap());
        let expected_fields: Vec<_> = projection_fields
            .into_iter()
            .map(String::from)
            .sorted()
            .collect();

        if let TransformSpec::Project(project) = tx {
            assert_eq!(project.fields, expected_fields);
        } else {
            panic!("Expected project transform")
        }
    }

    # [rstest(
        spec_name,
        projection_fields,
        case("vegalite/point_2d", vec!["Horsepower", "Miles_per_Gallon"]),
        case("vegalite/point_bubble", vec!["Acceleration", "Horsepower", "Miles_per_Gallon"]),
        case("vegalite/concat_marginal_histograms", vec![
            "IMDB Rating",
            "Rotten Tomatoes Rating",
        ]),
        case("vegalite/rect_binned_heatmap", vec![
            "IMDB Rating",
            "Rotten Tomatoes Rating",
        ]),
    )]
    fn test_get_column_usage(spec_name: &str, projection_fields: Vec<&str>) {
        // Load spec
        let spec_path = format!("{}/tests/specs/{}.vg.json", crate_dir(), spec_name);
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();
        let root_usages = get_column_usage(&spec).unwrap();
        let expected_fields: Vec<_> = projection_fields.into_iter().map(String::from).collect();
        assert_eq!(root_usages["source_0"], Some(expected_fields));
    }
}
