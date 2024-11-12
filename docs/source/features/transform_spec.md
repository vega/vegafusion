# Transform Spec

VegaFusion can be used to evaluate datasets in a Vega spec, remove unused columns, and inline the results in a transformed Vega spec. This transformed Vega spec is self-contained and may be displayed with the standard Vega JavaScript library.

This is the foundation of Vega-Altair's [``"vegafusion"`` data transformer](https://altair-viz.github.io/user_guide/large_datasets.html#vegafusion-data-transformer) when used with the default HTML or static image renderers. 

:::{warning}
The pre-transform process will, by default, preserve the interactive behavior of the input Vega specification. For interactive charts that perform filtering, this may result in the generation of a spec containing the full input dataset. If interactivity does not need to be preserved (e.g. if the resulting chart is used in a static context) then the ``preserve_interactivity`` option should be set to False. If interactivity is needed, then the [Chart State](./chart_state) workflow may be more appropriate.
:::

## Python

```{eval-rst}
.. automethod:: vegafusion.runtime.VegaFusionRuntime.pre_transform_spec
```

**Example**: See [pre_transform_spec.py](https://github.com/vega/vegafusion/tree/v2/examples/python-examples/pre_transform_spec.py) for a complete example.

## Rust

See [pre_transform_spec.rs](https://github.com/vega/vegafusion/tree/v2/examples/rust-examples/examples/pre_transform_spec.rs) for a complete example.
