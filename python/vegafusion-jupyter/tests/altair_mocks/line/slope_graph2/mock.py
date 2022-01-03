# https://altair-viz.github.io/gallery/slope_graph.html
# Using mean instead of median

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source).mark_line().encode(
    x='year:O',
    y='mean(yield)',
    color='site'
)
