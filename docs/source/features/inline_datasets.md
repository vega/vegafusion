# Inline Datasets
The VegaFusion transform methods ([data](./transform_data.md), [spec](./transform_spec.md), and [extract](./transform_extract.md)) and [chart state](./chart_state.md) all support an `inline_datasets` argument. This may be used to pass Arrow tables, DataFrames, or DataFusion logical plans into Vega specifications.

Vega-Altair's `"vegafusion"` data transformer uses this approach to reference DataFrames from Vega specs without writing them to disk or converting them to JSON.

## Overview
Vega specs may include `data` entries with a `url` of the form `"vegafusion+dataset://{dataset_name}`, where a dataset named `{dataset_name}` is expected to be provided using `inline_datasets`.

Here is an example Vega specification:

```
{
  ...
  "data": [
    {
      "name": "source0",
      "url": "vegafusion+dataset://movies",
      "transform": [
        ...
      ]
    }
  ],
  ...
}
```

In this case, VegaFusion expects that `inline_datasets` will contain a dataset named `movies`.

## Python
In Python, `inline_datasets` should be a `dict` from dataset names (e.g. `movies` in the example above) to DataFrames or Arrow tables. "DataFrames" may be of any type supported by [Narwhals](https://narwhals-dev.github.io/narwhals/) (including [pandas](https://pandas.pydata.org/), [Polars](https://pola.rs/), [PyArrow](https://arrow.apache.org/docs/python/index.html), [Vaex](https://vaex.io/), [Ibis](https://ibis-project.org/), etc.) and "Arrow tables" may be any object supporting the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) (e.g. [arro3](https://github.com/kylebarron/arro3), [nanoarrow](https://arrow.apache.org/nanoarrow/latest/getting-started/python.html), etc.).

In the case of types supported by Narwhals, VegaFusion will use [`get_column_usage`](./column_usage.md) to project down to the minimal collection of columns that are required, then rely on Narwhals' support for the Arrow PyCapsule API to convert these required columns to an `arro3` Arrow table for zero-copy transfer to Rust.

See [inline_datasets.py](https://github.com/vega/vegafusion/tree/v2/examples/python-examples/inline_datasets.py) for a complete example with pandas.

## Rust
In Rust, `inline_datasets` should be a `HashMap<String, VegaFusionDataset>` from dataset names (e.g. `movies` in the example above) to `VegaFusionDataset` instances. `VegaFusionDataset` is an enum that may be either a `VegaFusionTable` (which is a thin wrapper around Arrow RecordBatches), or a DataFusion [`LocalPlan`](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html) (which represents an arbitrary DataFusion query).

See [inline_datasets.rs](https://github.com/vega/vegafusion/tree/v2/examples/rust-examples/examples/inline_datasets.rs) for a complete example using a `VegaFusionTable`, and see [inline_datasets_plan.rs](https://github.com/vega/vegafusion/tree/v2/examples/rust-examples/examples/inline_datasets_plan.rs) for a complete example using a DataFusion ``LogicalPlan``.
