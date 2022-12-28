# https://altair-viz.github.io/gallery/multiple_marks.html

import altair as alt
from vega_datasets import data

source = data.stocks()

alt.Chart(source).mark_line(point=True).encode(
    x='date:T',
    y='price:Q',
    color='symbol:N'
)
