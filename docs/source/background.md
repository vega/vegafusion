# Background

## Vega, Vega-Lite, and Altair
VegaFusion is designed to complement the Vega visualization ecosystem. In particular, the [Vega](https://vega.github.io/), [Vega-Lite](https://vega.github.io/vega-lite/), and [Altair](https://altair-viz.github.io/) projects.  If you're not familiar with these projects, it will be helpful to take a few minutes to browse their documentation as background for understanding what VegaFusion adds.

## Transforms and Signals
One powerful feature of the Vega visualization grammar is that it includes a rich collection of data manipulation functions called [transforms](https://vega.github.io/vega/docs/transforms/).  Transforms have functionality that is similar to that provided by SQL queries or Pandas DataFrame operations, but they are specifically designed to cover data preprocessing tasks that are useful in constructing data visualizations.

For additional flexibility, Vega also provides the concept of [signals](https://vega.github.io/vega/docs/signals/). These are scalar variables that can be constructed using the [Vega expression language](https://vega.github.io/vega/docs/expressions/), which is a subset of JavaScript.  Transforms can accept and produce signals.

There are at least two significant advantages to having data transformations and signals included in a visualization specification.
- First, it makes it possible for a visualization to accept raw data files as input and then perform its own data cleaning and manipulation.  This often removes the need to generate temporary intermediary data files.
- Second, it enables higher-level libraries like Vega-Lite to automate the creation of rich interactive visualizations with features like [cross filtering](https://vega.github.io/vega-lite/examples/interactive_layered_crossfilter.html) and [drill down](https://altair-viz.github.io/gallery/select_detail.html).

## Motivation for VegaFusion
Vega makes it possible to create declarative JSON specifications of rich interactive visualizations that are fully self-contained. They can run entirely in a web browser without requiring access to an external database or a Python library like Pandas.

For datasets of a few tens of thousands rows or fewer, this architecture often results in extremely smooth and responsive interactivity. However, this architecture doesn't scale very well to datasets of hundreds of thousands of rows or more.  This is the gap VegaFusion aims to fill.