# https://altair-viz.github.io/gallery/normalized_stacked_area_chart.html

import altair as alt
from vega_datasets import data

source = data.iowa_electricity()

alt.Chart(source).mark_area().encode(
    x="year:T",
    y=alt.Y("net_generation:Q", stack="normalize"),
    color="source:N"
)
