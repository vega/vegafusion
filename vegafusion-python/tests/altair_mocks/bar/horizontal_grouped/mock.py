# https://altair-viz.github.io/gallery/grouped_bar_chart_horizontal.html

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source).mark_bar().encode(
    x='sum(yield):Q',
    y='year:O',
    color='year:N',
    row='site:N'
)
