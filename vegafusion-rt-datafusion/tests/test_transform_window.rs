#[macro_use]
extern crate lazy_static;

mod util;
use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use util::equality::TablesEqualConfig;

use rstest::rstest;
use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
use vegafusion_core::spec::transform::TransformSpec;

// For some reason this test is especially slow on Windows on CI.
// Skip for now.
#[cfg(not(target_os = "windows"))]
mod test_window_single {
    use crate::*;
    use serde_json::json;

    #[rstest]
    fn test(
        #[values(
            AggregateOpSpec::Count,
            AggregateOpSpec::Sum,
            AggregateOpSpec::Mean,
            AggregateOpSpec::Average,
            AggregateOpSpec::Min,
            AggregateOpSpec::Max,
            AggregateOpSpec::Stdev,
            AggregateOpSpec::Variance,
            AggregateOpSpec::Stdevp,
            AggregateOpSpec::Variancep
        )]
        op: AggregateOpSpec,

        #[values(
            json!([null, 0]),
            json!([-5, 4]),
            json!([null, null]),
        )]
        frame: serde_json::Value,

        #[values(true, false)] ignore_peers: bool,
    ) {
        if frame == json!([null, 0])
            && matches!(op, AggregateOpSpec::Stdevp | AggregateOpSpec::Variancep)
        {
            // Vega and DataFusion differ on how to handle pop variance of single element.
            // DataFusion returns 0 while Vega returns null
            return;
        }

        let dataset = vega_json_dataset("movies");

        let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!(
            [
                {
                    "type": "filter",
                    "expr": "isValid(datum['IMDB Rating']) && isValid(datum['Title']) && isValid(datum['Rotten Tomatoes Rating'])"
                },
                {
                    "type": "window",
                    "params": [null],
                    "as": ["Cumulative Count"],
                    "ops": [op],
                    "fields": ["IMDB Rating"],
                    "sort": {
                        "field": ["IMDB Rating", "Title", "Rotten Tomatoes Rating"],
                        "order": ["ascending", "ascending", "ascending"]
                    },
                    "frame": frame,
                    "ignorePeers": ignore_peers,
                },
                {
                    "type": "collect",
                    "sort": {
                        "field": ["IMDB Rating", "Title", "Rotten Tomatoes Rating"],
                        "order": ["ascending", "ascending", "ascending"]
                    },
                }
            ]
        )).unwrap();

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
            ..Default::default()
        };

        check_transform_evaluation(
            &dataset,
            transform_specs.as_slice(),
            &comp_config,
            &eq_config,
        );
    }
}
