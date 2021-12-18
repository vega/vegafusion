# https://altair-viz.github.io/gallery/filled_step_chart.html

import altair as alt
from vega_datasets import data

source = data.stocks()

alt.Chart(source).mark_area(
    color="lightblue",
    interpolate='step-after',
    line=True
).encode(
    x='date',
    y='price'
).transform_filter(alt.datum.symbol == 'GOOG')
