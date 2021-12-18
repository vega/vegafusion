# https://altair-viz.github.io/gallery/interactive_brush.html

import altair as alt
from vega_datasets import data

source = data.cars()
brush = alt.selection(type='interval')

alt.Chart(source).mark_point().encode(
    x='Horsepower:Q',
    y='Miles_per_Gallon:Q',
    color=alt.condition(brush, 'Cylinders:O', alt.value('grey')),
).add_selection(brush)
