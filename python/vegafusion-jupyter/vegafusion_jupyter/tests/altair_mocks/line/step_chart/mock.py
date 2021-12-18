# https://altair-viz.github.io/gallery/step_chart.html

import altair as alt
from vega_datasets import data

source = data.stocks()

alt.Chart(source).mark_line(interpolate='step-after').encode(
    x='date',
    y='price'
).transform_filter(
    alt.datum.symbol == 'GOOG'
)
