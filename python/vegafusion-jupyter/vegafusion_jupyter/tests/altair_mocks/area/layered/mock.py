# https://altair-viz.github.io/gallery/layered_area_chart.html

import altair as alt
from vega_datasets import data

source = data.iowa_electricity()

alt.Chart(source).mark_area(opacity=0.3).encode(
    x="year:T",
    y=alt.Y("net_generation:Q", stack=None),
    color="source:N"
)
