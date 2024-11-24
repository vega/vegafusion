# Chart State

The Chart State workflow can be used to support interactive charts with transforms that are updated interactively. For example, for a chart that implements crossfiltering the `filter` transform must be re-evaluated repeatedly against the input dataset. 

This is the foundation of Vega-Altair's [JupyterChart](https://altair-viz.github.io/user_guide/jupyter_chart.html) when combined with the [``"vegafusion"`` data transformer](https://altair-viz.github.io/user_guide/large_datasets.html#vegafusion-data-transformer).

## Python
```{eval-rst}
.. automethod:: vegafusion.runtime.VegaFusionRuntime.new_chart_state
```

**Example**: See [chart_state.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/chart_state.py) for a complete example.

## Rust
See [chart_state.rs](https://github.com/vega/vegafusion/tree/main/examples/rust-examples/examples/chart_state.rs) for a complete example.
