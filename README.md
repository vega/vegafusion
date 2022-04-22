# VegaFusion
VegaFusion provides serverside acceleration for the [Vega](https://vega.github.io/) visualization grammar. While not limited to Python, an initial application of VegaFusion is the acceleration of the [Altair](https://altair-viz.github.io/) Python interface to [Vega-Lite](https://vega.github.io/vega-lite/).

The core VegaFusion algorithms are implemented in Rust. Python integration is provided using [PyO3](https://pyo3.rs/v0.15.1/) and JavaScript integration is provided using [wasm-bindgen](https://github.com/rustwasm/wasm-bindgen).

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/vegafusion/demos/HEAD?labpath=notebooks)

## Documentation
See the documentation at https://vegafusion.io

## Project Status
VegaFusion is a young project, but it is already fairly well tested. The integration test suite includes image comparisons with over 600 specifications from the Vega, Vega-Lite, and Altair galleries.

## Quick Start: Serverside acceleration for Altair in Jupyter
VegaFusion can be used to provide serverside acceleration for Altair visualizations when displayed in Jupyter contexts (Classic notebook, JupyterLab, and Voila). First, install the `vegafusion-jupyter` package, along with `vega-datasets` for the example below.

```bash
pip install vegafusion-jupyter vega-datasets
```

Then, open a jupyter notebook (either the classic notebook, or a notebook inside JupyterLab) and run these two lines to import and enable VegaFusion

```python
import vegafusion as vf
vf.jupyter.enable()
```
VegaFusion will now be used to accelerate any Altair chart. For example, here's the [interactive average](https://altair-viz.github.io/gallery/selection_layer_bar_month.html) Altair gallery example.

```python
import altair as alt
from vega_datasets import data

source = data.seattle_weather()
brush = alt.selection(type='interval', encodings=['x'])

bars = alt.Chart().mark_bar().encode(
    x='month(date):O',
    y='mean(precipitation):Q',
    opacity=alt.condition(brush, alt.OpacityValue(1), alt.OpacityValue(0.7)),
).add_selection(
    brush
)

line = alt.Chart().mark_rule(color='firebrick').encode(
    y='mean(precipitation):Q',
    size=alt.SizeValue(3)
).transform_filter(
    brush
)

chart = alt.layer(bars, line, data=source)
chart
```

https://user-images.githubusercontent.com/15064365/148408648-43a5cfd0-b0d8-456e-a77a-dd344d8d07df.mov

Histogram binning, aggregation, selection filtering, and average calculations will now be evaluated in the Python kernel process with efficient parallelization, rather than in the single-threaded browser context.

You can see that VegaFusion acceleration is working by noticing that the Python [kernel is running](https://experienceleague.adobe.com/docs/experience-platform/data-science-workspace/jupyterlab/overview.html?lang=en#kernel-sessions) as the selection region is created or moved. You can also notice the VegaFusion logo in the dropdown menu button.

## Motivation for VegaFusion
Vega makes it possible to create declarative JSON specifications for rich interactive visualizations that are fully self-contained. They can run entirely in a web browser without requiring access to an external database or a Python kernel.

For datasets of a few thousand rows or fewer, this architecture results in extremely smooth and responsive interactivity. However, this architecture does not scale very well to datasets of hundreds of thousands of rows or more.  This is the problem that VegaFusion aims to solve.

## DataFusion integration
[Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) is an SQL compatible query engine that integrates with the Rust implementation of Apache Arrow.  VegaFusion uses DataFusion to implement many of the Vega transforms, and it compiles the Vega expression language directly into the DataFusion expression language.  In addition to being quite fast, a particularly powerful characteristic of DataFusion is that it provides many interfaces that can be extended with custom Rust logic.  For example, VegaFusion defines many custom UDFs that are designed to implement the precise semantics of the Vega expression language and the Vega expression functions.

# License and Copyright
VegaFusion is released under the [AGPLv3 license](https://www.gnu.org/licenses/agpl-3.0.en.html).  This is a copy-left license in the GPL family of licenses. As with all [OSI approved licenses](https://opensource.org/licenses/alphabetical), there are no restrictions on what code licensed under AGPLv3 can be used for. However, the requirements for what must be shared publicly are greater than for licenses that are more commonly used in the Python ecosystem like [Apache-2](https://opensource.org/licenses/Apache-2.0), [MIT](https://opensource.org/licenses/MIT), and [BSD-3](https://opensource.org/licenses/BSD-3-Clause).

The VegaFusion copyright is owned by _VegaFusion Technologies LLC_, and contributors are asked to sign a Contributor License Agreement (based on the Oracle CLA) that grants the company the non-exclusive right to re-license the contribution in the future. For example, the project could be re-licensed to one of the more permissive licenses above, or it could be dual licensed with a commercial license as a means to support the project.

If you might be interested in using VegaFusion in a context where the AGPL license is prohibitive, please [get in touch](mailto:jon@vegafusion.io).

# About the Name
There are two meanings behind the name "VegaFusion"
- It's a reference to the [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) library which is used to implement many of the supported Vega transforms
- Vega and Altair are named after stars, and stars are powered by nuclear fusion

# Building VegaFusion
If you're interested in building VegaFusion from source, see [BUILD.md](BUILD.md)

# Roadmap
Supporting serverside acceleration for Altair in Jupyter was chosen as the first application of VegaFusion, but there are a lot of exciting ways that VegaFusion can be extended in the future.  For more information, see the [Roadmap](https://vegafusion.io/roadmap.html).

