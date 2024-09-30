# https://altair-viz.github.io/gallery/trellis_histogram.html
# Modification: Set height=100 to keep the screenshot from extending outside the browser window

import altair as alt
from vega_datasets import data

source = data.cars()

alt.Chart(source).mark_bar().encode(
    alt.X("Horsepower:Q", bin=True),
    y='count()',
    row='Origin'
).properties(height=100)
