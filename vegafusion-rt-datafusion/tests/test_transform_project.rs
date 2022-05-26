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
mod test_project {
    use crate::util::check::check_transform_evaluation;
    use crate::util::datasets::vega_json_dataset;
    use rstest::rstest;
    use vegafusion_core::spec::transform::project::ProjectTransformSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest(
        fields,
        case(vec!["Beak Length (mm)", "Species"]),
        case(vec!["Species", "Beak Length (mm)"]),
        case(vec!["Species", "Beak Length (mm)", "Bogus"]),
    )]
    fn test(fields: Vec<&str>) {
        let dataset = vega_json_dataset("penguins");

        let fields: Vec<_> = fields.iter().map(|s| s.to_string()).collect();
        let project_spec = ProjectTransformSpec {
            fields,
            extra: Default::default(),
        };
        let transform_specs = vec![TransformSpec::Project(project_spec)];

        let comp_config = Default::default();
        let eq_config = Default::default();

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
