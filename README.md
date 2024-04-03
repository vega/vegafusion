<img src="https://user-images.githubusercontent.com/15064365/213880031-eca5ab61-3d97-474f-a0e6-3662dbc9887f.svg#gh-dark-mode-only" width=50%>

<img src="https://user-images.githubusercontent.com/15064365/213880036-3d28c1b6-5b76-47c4-a010-2a623522c9f2.svg#gh-light-mode-only" width=50%>

---

VegaFusion provides serverside acceleration for the [Vega](https://vega.github.io/) visualization grammar. While not limited to Python, an initial application of VegaFusion is the acceleration of the [Vega-Altair](https://altair-viz.github.io/) Python interface to [Vega-Lite](https://vega.github.io/vega-lite/).

The core VegaFusion algorithms are implemented in Rust. Python integration is provided using [PyO3](https://pyo3.rs/v0.15.1/) and JavaScript integration is provided using [wasm-bindgen](https://github.com/rustwasm/wasm-bindgen).

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hex-inc/vegafusion-demos/HEAD?labpath=notebooks)

## Documentation
See the documentation at https://vegafusion.io

## History
VegaFusion was developed by Jon Mease and acquired by [Hex Technologies](https://hex.tech/) in 2022. Hex donated VegaFusion to the Vega Project in 2024 and continues to support its development and maintenance.

VegaFusion's integration with Vega-Altair was initially developed outside of Altair in the `vegafusion` Python package. As of Vega-Altair version 5.3, all of these integrations have been incorporated into the upstream Vega-Altair package.

## Quickstart 1: Overcome `MaxRowsError` with VegaFusion
The Vega-Altair [`"vegafusion"` data transformer](https://altair-viz.github.io/user_guide/large_datasets.html#vegafusion-data-transformer) can be used to overcome the Altair [`MaxRowsError`](https://altair-viz.github.io/user_guide/faq.html#maxrowserror-how-can-i-plot-large-datasets) by performing data-intensive aggregations on the server and pruning unused columns from the source dataset. First install the `altiar` Python package with the `all` extras enabled

```bash
pip install "altair[all]>=5.3"
```

Then open a Jupyter notebook (either the classic notebook or a notebook inside JupyterLab), and create an Altair histogram of a 1 million row flights dataset

```python
import pandas as pd
import altair as alt

flights = pd.read_parquet(
    "https://vegafusion-datasets.s3.amazonaws.com/vega/flights_1m.parquet"
)

delay_hist = alt.Chart(flights).mark_bar().encode(
    alt.X("delay", bin=alt.Bin(maxbins=30)),
    alt.Y("count()")
)
delay_hist
```
```
---------------------------------------------------------------------------
MaxRowsError: The number of rows in your dataset is greater than the maximum allowed (5000).

Try enabling the VegaFusion data transformer which raises this limit by pre-evaluating data
transformations in Python.
    >> import altair as alt
    >> alt.data_transformers.enable("vegafusion")

Or, see https://altair-viz.github.io/user_guide/large_datasets.html for additional information
on how to plot large datasets.
```

This results in an Altair `MaxRowsError`, as by default Altair is configured to allow no more than 5,000 rows of data to be sent to the browser.  This is a safety measure to avoid crashing the user's browser.  The `"vegafusion"` data transformer can be used to overcome this limitation by performing data intensive transforms (e.g. filtering, binning, aggregation, etc.) in the Python kernel before the resulting data is sent to the web browser.

The `"vegafusion"` data transformer is enabled like this:

```python
alt.data_transformers.enable("vegafusion")
```

Now the chart displays quickly without errors
```
delay_hist
```
![Flight Delay Histogram](https://user-images.githubusercontent.com/15064365/209973961-948b9d10-4202-4547-bbc8-d1981dcc8c4e.png)

## Quickstart 2: Extract transformed data
By default, data transforms in an Altair chart (e.g. filtering, binning, aggregation, etc.) are performed by the Vega JavaScript library running in the browser. This has the advantage of making the charts produced by Altair fully standalone, not requiring access to a running Python kernel to render properly. But it has the disadvantage of making it difficult to access the transformed data (e.g. the histogram bin edges and count values) from Python.  Since VegaFusion evaluates these transforms in the Python kernel, it's possible to access them from Python using the `chart.transformed_data()` function.

For example, the following code demonstrates how to access the histogram bin edges and counts for the example above:

```python
import pandas as pd
import altair as alt

flights = pd.read_parquet(
    "https://vegafusion-datasets.s3.amazonaws.com/vega/flights_1m.parquet"
)

delay_hist = alt.Chart(flights).mark_bar().encode(
    alt.X("delay", bin=alt.Bin(maxbins=30)),
    alt.Y("count()")
)
delay_hist.transformed_data()
```
|    |   bin_maxbins_30_delay |   bin_maxbins_30_delay_end |   __count |
|---:|-----------------------:|---------------------------:|----------:|
|  0 |                    -20 |                          0 |    419400 |
|  1 |                     80 |                        100 |     11000 |
|  2 |                      0 |                         20 |    392700 |
|  3 |                     40 |                         60 |     38400 |
|  4 |                     60 |                         80 |     21800 |
|  5 |                     20 |                         40 |     92700 |
|  6 |                    100 |                        120 |      5300 |
|  7 |                    -40 |                        -20 |      9900 |
|  8 |                    120 |                        140 |      3300 |
|  9 |                    140 |                        160 |      2000 |
| 10 |                    160 |                        180 |      1800 |
| 11 |                    320 |                        340 |       100 |
| 12 |                    180 |                        200 |       900 |
| 13 |                    240 |                        260 |       100 |
| 14 |                    -60 |                        -40 |       100 |
| 15 |                    260 |                        280 |       100 |
| 16 |                    200 |                        220 |       300 |
| 17 |                    360 |                        380 |       100 |

## Quickstart 3: Accelerate interactive charts

As shown above, the `"vegafusion"` data transformer can be combined with Vega-Altair's standard [renderers](https://altair-viz.github.io/user_guide/display_frontends.html), and this configuration will work well to scale non-interactive charts. However, because the standard renderers do not support passing information from the browser back to the Python kernel, VegaFusion is unable to evaluate transforms that are referenced by selections. To support this use case, the `"vegafusion"` data transformer may be combined with Altair's [`JupyterChart`](https://altair-viz.github.io/user_guide/jupyter_chart.html). Because `JupyterChart` (when used in an environment that support Jupyter Widgets) provides a two-way connection between the browser and the Python kernel, selection operations (e.g. filtering to the extents of a brush selection) can be evaluated interactively in the Python kernel, which eliminates the need to transfer the full dataset to the browser in order to maintain interactivity.

This mode is activate by enabling the `"vegafusion"` data transformer and either enabling the `"jupyter"` renderer, or using `JupyterChart` directly.

```python
import pandas as pd
import altair as alt

alt.data_transformers.enable("vegafusion")
alt.renderers.enable("jupyter")

flights = pd.read_parquet(
    "https://vegafusion-datasets.s3.amazonaws.com/vega/flights_1m.parquet"
)

brush = alt.selection_interval(encodings=['x'])

# Define the base chart, with the common parts of the
# background and highlights
base = alt.Chart().mark_bar().encode(
    x=alt.X(alt.repeat('column'), type='quantitative', bin=alt.Bin(maxbins=20)),
    y='count()'
).properties(
    width=160,
    height=130
)

# gray background with selection
background = base.encode(
    color=alt.value('#ddd')
).add_params(brush)

# blue highlights on the selected data
highlight = base.transform_filter(brush)

# layer the two charts & repeat
chart = alt.layer(
    background,
    highlight,
    data=flights
).transform_calculate(
    "time",
    "hours(datum.date)"
).repeat(column=["distance", "delay", "time"])

chart
```

https://user-images.githubusercontent.com/15064365/209974420-480121b4-b206-4bb2-b473-0c663e38ea5e.mov

Histogram binning, aggregation, and selection filtering are now evaluated in the Python kernel process with efficient parallelization, and only the aggregated data (one row per histogram bar) is sent to the browser.

You can see that the JupyterChart widget maintains a live connection to the Python kernel by noticing that the Python [kernel is running](https://experienceleague.adobe.com/docs/experience-platform/data-science-workspace/jupyterlab/overview.html?lang=en#kernel-sessions) as the selection region is created or moved.

## Motivation for VegaFusion
Vega makes it possible to create declarative JSON specifications for rich interactive visualizations that are fully self-contained. They can run entirely in a web browser without requiring access to an external database or a Python kernel.

For datasets of a few thousand rows or fewer, this architecture results in extremely smooth and responsive interactivity. However, this architecture does not scale very well to datasets of hundreds of thousands of rows or more.  This is the problem that VegaFusion aims to solve.

## DataFusion integration
[Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) is an SQL compatible query engine that integrates with the Rust implementation of Apache Arrow.  VegaFusion uses DataFusion to implement many of the Vega transforms, and it compiles the Vega expression language directly into the DataFusion expression language.  In addition to being quite fast, a particularly powerful characteristic of DataFusion is that it provides many interfaces that can be extended with custom Rust logic.  For example, VegaFusion defines many custom UDFs that are designed to implement the precise semantics of the Vega expression language and Vega expression functions.

# License
VegaFusion is licensed under the [BSD-3](https://opensource.org/licenses/BSD-3-Clause) license. This is the same license used by Vega, Vega-Lite, and Vega-Altair.

# About the Name
There are two meanings behind the name "VegaFusion"
- It's a reference to the [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) library which is used to implement many of the supported Vega transforms
- Vega and Altair are named after stars, and stars are powered by nuclear fusion

# Building VegaFusion
If you're interested in building VegaFusion from source, see [BUILD.md](BUILD.md)

# Roadmap
Supporting serverside acceleration for Altair in Jupyter was chosen as the first application of VegaFusion, but there are a lot of exciting ways that VegaFusion can be extended in the future.  For more information, see the [Roadmap](https://vegafusion.io/roadmap.html).
