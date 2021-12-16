# https://altair-viz.github.io/gallery/trellis_stacked_bar_chart.html

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source).mark_bar().encode(
    column='year',
    x='yield',
    y='variety',
    color='site'
).properties(width=220)