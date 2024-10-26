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
mod test_window_single_agg {
    use crate::*;
    use serde_json::json;
    use vegafusion_core::spec::transform::window::{WindowOpSpec, WindowTransformOpSpec};

    #[rstest]
    fn test(
        #[values(
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Count),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Sum),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Mean),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Average),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Min),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Max),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Stdev),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Variance),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Stdevp),
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Variancep),
            WindowTransformOpSpec::Window(WindowOpSpec::RowNumber),
            WindowTransformOpSpec::Window(WindowOpSpec::Rank),
            WindowTransformOpSpec::Window(WindowOpSpec::DenseRank),
            WindowTransformOpSpec::Window(WindowOpSpec::PercentileRank),
            WindowTransformOpSpec::Window(WindowOpSpec::CumeDist),
            WindowTransformOpSpec::Window(WindowOpSpec::FirstValue),
            WindowTransformOpSpec::Window(WindowOpSpec::LastValue)
        )]
        op: WindowTransformOpSpec,

        #[values(
            json!([null, 0]),
            json!([-5, 4]),
            json!([null, null]),
        )]
        frame: serde_json::Value,

        #[values(true, false)] ignore_peers: bool,
    ) {
        // Vega and DataFusion differ on how to handle pop variance and percentile rank of
        // single element DataFusion returns 0 while Vega returns null.
        let null_matches_zero = matches!(
            op,
            WindowTransformOpSpec::Aggregate(AggregateOpSpec::Stdevp)
                | WindowTransformOpSpec::Aggregate(AggregateOpSpec::Variancep)
                | WindowTransformOpSpec::Window(WindowOpSpec::PercentileRank)
        );

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
                    "as": ["Window Result"],
                    "ops": [op],
                    "fields": ["IMDB Rating"],
                    "groupby": ["MPAA Rating"],
                    "sort": {
                        "field": ["Title", "Rotten Tomatoes Rating", "IMDB Rating"],
                        "order": ["ascending", "ascending", "ascending"]
                    },
                    "frame": frame,
                    "ignorePeers": ignore_peers,
                },
                {
                    "type": "project",
                    "fields": ["MPAA Rating", "IMDB Rating", "Title", "Rotten Tomatoes Rating", "Window Result"]
                },
                {
                    "type": "collect",
                    "sort": {
                        "field": ["MPAA Rating", "Title", "Rotten Tomatoes Rating", "IMDB Rating"],
                        "order": ["ascending", "ascending", "ascending", "ascending"]
                    },
                }
            ]
        )).unwrap();

        let comp_config = Default::default();

        let eq_config = TablesEqualConfig {
            row_order: true,
            null_matches_zero,
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
