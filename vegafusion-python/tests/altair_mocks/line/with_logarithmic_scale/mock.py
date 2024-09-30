# https://altair-viz.github.io/gallery/line_with_log_scale.html

import altair as alt
from vega_datasets import data

source = data.population()

alt.Chart(source).mark_line().encode(
    x='year:O',
    y=alt.Y(
        'sum(people)',
        scale=alt.Scale(type="log")  # Here the scale is applied
    )
)
