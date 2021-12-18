# https://altair-viz.github.io/gallery/line_with_ci.html
# extent changed for "ci" to "stdev" to be deterministic

import altair as alt
from vega_datasets import data

source = data.cars()

line = alt.Chart(source).mark_line().encode(
    x='Year',
    y='mean(Miles_per_Gallon)'
)

band = alt.Chart(source).mark_errorband(extent='stdev').encode(
    x='Year',
    y=alt.Y('Miles_per_Gallon', title='Miles/Gallon'),
)

band + line
