#[cfg(test)]
mod test_custom_specs {
    use crate::crate_dir;
    use rstest::rstest;
    use std::fs;
    use vegafusion_core::planning::plan::{PlannerConfig, SpecPlan};
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
    fn test(spec_name: &str, data_index: usize, projection_fields: Vec<&str>) {
        // Load spec
        let spec_path = format!("{}/tests/specs/{}.vg.json", crate_dir(), spec_name);
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        let planner_config = PlannerConfig {
            projection_pushdown: true,
            ..Default::default()
        };
        let spec_plan = SpecPlan::try_new(&spec, &planner_config).unwrap();
        let data = &spec_plan.server_spec.data[data_index];
        let tx = &data.transform[data.transform.len() - 1];

        // Print data
        // println!("{}", serde_json::to_string_pretty(&spec_plan.server_spec.data).unwrap());

        if let TransformSpec::Project(project) = tx {
            let expected_fields: Vec<_> = projection_fields.iter().map(|f| f.to_string()).collect();
            assert_eq!(project.fields, expected_fields);
        } else {
            panic!("Expected project transform")
        }
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
