# https://altair-viz.github.io/gallery/anscombe_plot.html

import altair as alt
from vega_datasets import data

source = data.anscombe()

alt.Chart(source).mark_circle().encode(
    alt.X('X', scale=alt.Scale(zero=False)),
    alt.Y('Y', scale=alt.Scale(zero=False)),
    alt.Facet('Series', columns=2),
).properties(
    width=180,
    height=180,
)
