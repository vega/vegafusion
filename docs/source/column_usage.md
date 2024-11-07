# Column Usage
VegaFusion provides a function for introspecting a Vega specification and determining which columns are referenced from each root dataset. A root dataset is one defined at the top-level of the spec that includes a `url` or `values` properties. This is useful in contexts where it's more efficient to minimize the number of columns provided to the Vega specification. For example, the Python library uses this function to determine how to downsample the input DataFrame columns prior to converting to Arrow.

When VegaFusion cannot precisely determine which columns are referenced from each root dataset, this function returns `None` or `null` for the corresponding dataset.

## Python
```{eval-rst}
.. autofunction:: vegafusion.get_column_usage
```

See [column_usage.py](https://github.com/vega/vegafusion/tree/v2/examples/python-examples/column_usage.py) for a complete example.

## Rust
See [column_usage.rs](https://github.com/vega/vegafusion/tree/v2/examples/rust-examples/examples/column_usage.rs) for a complete example.

## JavaScript
See the [Editor Demo](https://github.com/vega/vegafusion/tree/v2/examples/editor-demo/src/index.js) for example usage of the `getColumnUsage` function in the `vegafusion-wasm` package.
