# https://altair-viz.github.io/gallery/bar_chart_sorted.html

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source).mark_bar().encode(
    x='sum(yield):Q',
    y=alt.Y('site:N', sort='-x')
)
