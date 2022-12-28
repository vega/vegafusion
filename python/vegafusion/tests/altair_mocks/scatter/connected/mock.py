# https://altair-viz.github.io/gallery/connected_scatterplot.html

import altair as alt
from vega_datasets import data

source = data.driving()

alt.Chart(source).mark_line(point=True).encode(
    alt.X('miles', scale=alt.Scale(zero=False)),
    alt.Y('gas', scale=alt.Scale(zero=False)),
    order='year'
)
