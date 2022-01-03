# https://altair-viz.github.io/gallery/trellis_scatter_plot.html
# With reduced height

import altair as alt
from vega_datasets import data

source = data.cars()

alt.Chart(source).mark_point().encode(
    x='Horsepower:Q',
    y='Miles_per_Gallon:Q',
    row='Origin:N'
).properties(height=150)
