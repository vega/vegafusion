# Transform Data

VegaFusion can be used to evaluate datasets in a Vega spec and return them as arrow tables or DataFrames. This is the foundation for Vega-Altair's [`chart.transformed_data`](https://altair-viz.github.io/user_guide/transform/index.html#accessing-transformed-data) method.

## Python

```{eval-rst}
.. automethod:: vegafusion.runtime.VegaFusionRuntime.pre_transform_datasets
```

**Example**: See [pre_transform_data.py](https://github.com/vega/vegafusion/tree/v2/examples/python-examples/pre_transform_data.py) for a complete example.

## Rust
The Rust API provides a slightly more general `pre_transform_values` method that can extract dataset or signal values.

See [pre_transform_data.rs](https://github.com/vega/vegafusion/tree/v2/examples/rust-examples/examples/pre_transform_data.rs) for a complete example of extracting dataset values as arrow tables.
