# https://altair-viz.github.io/gallery/trail_marker.html

import altair as alt
from vega_datasets import data

source = data.wheat()

alt.Chart(source).mark_trail().encode(
    x='year:T',
    y='wheat:Q',
    size='wheat:Q'
)