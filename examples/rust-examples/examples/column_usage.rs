use vegafusion_core::{get_column_usage, spec::chart::ChartSpec};

/// This example demonstrates how to use the `get_column_usage` function to get the names
/// of columns of each root dataset that are referenced in a Vega specification.
fn main() {
    let spec = get_spec();
    let column_usage = get_column_usage(&spec).unwrap();
    println!("{:#?}", column_usage);

    assert_eq!(
        column_usage,
        std::collections::HashMap::from([(
            "source".to_string(),
            Some(vec![
                "Acceleration".to_string(),
                "Horsepower".to_string(),
                "Miles_per_Gallon".to_string(),
            ])
        )])
    );
}

fn get_spec() -> ChartSpec {
    let spec_str = r##"
    {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "description": "A basic scatter plot example depicting automobile statistics.",
        "width": 200,
        "height": 200,
        "padding": 5,

        "data": [
            {
            "name": "source",
            "url": "data/cars.json",
            "transform": [
                {
                "type": "filter",
                "expr": "datum['Horsepower'] != null && datum['Miles_per_Gallon'] != null && datum['Acceleration'] != null"
                }
            ]
            }
        ],

        "scales": [
            {
            "name": "x",
            "type": "linear",
            "round": true,
            "nice": true,
            "zero": true,
            "domain": {"data": "source", "field": "Horsepower"},
            "range": "width"
            },
            {
            "name": "y",
            "type": "linear",
            "round": true,
            "nice": true,
            "zero": true,
            "domain": {"data": "source", "field": "Miles_per_Gallon"},
            "range": "height"
            },
            {
            "name": "size",
            "type": "linear",
            "round": true,
            "nice": false,
            "zero": true,
            "domain": {"data": "source", "field": "Acceleration"},
            "range": [4,361]
            }
        ],

        "axes": [
            {
            "scale": "x",
            "grid": true,
            "domain": false,
            "orient": "bottom",
            "tickCount": 5,
            "title": "Horsepower"
            },
            {
            "scale": "y",
            "grid": true,
            "domain": false,
            "orient": "left",
            "titlePadding": 5,
            "title": "Miles_per_Gallon"
            }
        ],

        "legends": [
            {
            "size": "size",
            "title": "Acceleration",
            "format": "s",
            "symbolStrokeColor": "#4682b4",
            "symbolStrokeWidth": 2,
            "symbolOpacity": 0.5,
            "symbolType": "circle"
            }
        ],

        "marks": [
            {
            "name": "marks",
            "type": "symbol",
            "from": {"data": "source"},
            "encode": {
                "update": {
                "x": {"scale": "x", "field": "Horsepower"},
                "y": {"scale": "y", "field": "Miles_per_Gallon"},
                "size": {"scale": "size", "field": "Acceleration"},
                "shape": {"value": "circle"},
                "strokeWidth": {"value": 2},
                "opacity": {"value": 0.5},
                "stroke": {"value": "#4682b4"},
                "fill": {"value": "transparent"}
                }
            }
            }
        ]
    }
    "##;
    serde_json::from_str(spec_str).unwrap()
}
