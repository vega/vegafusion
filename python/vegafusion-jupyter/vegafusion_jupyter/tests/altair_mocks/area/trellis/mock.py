# https://altair-viz.github.io/gallery/trellis_area.html

import altair as alt
from vega_datasets import data

source = data.iowa_electricity()

alt.Chart(source).mark_area().encode(
    x="year:T",
    y="net_generation:Q",
    color="source:N",
    row="source:N"
).properties(
    height=100
)
