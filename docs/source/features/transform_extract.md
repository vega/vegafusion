# Transform Extract

The `pre_transform_extract` method generates a transformed spec like the [`pre_transform_spec`](./transform_spec.md) method, but instead of inlining the transformed datasets in the spec, these datasets are returned separately in arrow table format. This can be useful in contexts where the inline datasets are large, and it's possible to transmit them more efficiently in arrow format. 

## Python

```{eval-rst}
.. automethod:: vegafusion.runtime.VegaFusionRuntime.pre_transform_extract
```

**Example**: See [pre_transform_extract.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/pre_transform_extract.py) for a complete example.

## Rust

See [pre_transform_extract.rs](https://github.com/vega/vegafusion/tree/main/examples/rust-examples/examples/pre_transform_extract.rs) for a complete example.
