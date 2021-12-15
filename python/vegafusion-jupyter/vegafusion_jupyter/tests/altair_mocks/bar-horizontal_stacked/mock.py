# https://altair-viz.github.io/gallery/horizontal_stacked_bar_chart.html

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source).mark_bar().encode(
    x='sum(yield)',
    y='variety',
    color='site'
)
