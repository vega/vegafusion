# https://altair-viz.github.io/gallery/bar_chart_horizontal.html
# Height lowered to fit better

import altair as alt
from vega_datasets import data

source = data.wheat()

alt.Chart(source).mark_bar().encode(
    x='wheat:Q',
    y="year:O"
).properties(height=500)
