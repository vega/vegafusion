```{raw} html
<div
    style="margin-left:-16px;margin-bottom:-12px"
>
    <img
        src="_static/VegaFusionLightTitle.svg"
        class="only-light"
        width=300px
    >
    <img
        src="_static/VegaFusionDarkTitle.svg"
        class="only-dark"
        width=300px
    >
</div>
<h4>Building blocks for scaling Vega visualizations</h4>
```

# Purpose

The VegaFusion project provides Rust, Python, and JavaScript libraries for analyzing and scaling [Vega](https://vega.github.io/vega/) visualizations. The goal is to provide low-level building blocks that higher level Vega systems (such as [Vega-Altair](https://altair-viz.github.io/) in Python) can integrate with.

:::{note}
If you've arrived here looking for information on how to scale Vega-Altair visualizations to support larger datasets, see the Vega-Altair documentation on the [`"vegafusion"` data transformer](https://altair-viz.github.io/user_guide/large_datasets.html#vegafusion-data-transformer).
:::

# Python Installation

The VegaFusion Python package can be installed into a Python environment using pip

```bash
pip install vegafusion
```

or conda

```bash
conda install -c conda-forge vegafusion
```

```{toctree}
:maxdepth: 1
:hidden: true

features/features
Vega Coverage <vega_coverage/supported_transforms>
About <about/background>
Community <community/governance>
Blog <posts/posts>
```
