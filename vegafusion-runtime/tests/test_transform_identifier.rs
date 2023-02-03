#[macro_use]
extern crate lazy_static;

mod util;

use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use vegafusion_core::spec::transform::identifier::IdentifierTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;

#[test]
fn test_identifier() {
    let dataset = vega_json_dataset("penguins");
    let tx_spec = IdentifierTransformSpec {
        as_: "id".to_string(),
        extra: Default::default(),
    };

    let transform_specs = vec![TransformSpec::Identifier(tx_spec)];

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}
