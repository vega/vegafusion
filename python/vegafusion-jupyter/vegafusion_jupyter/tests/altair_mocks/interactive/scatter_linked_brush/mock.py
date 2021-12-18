# https://altair-viz.github.io/gallery/scatter_linked_brush.html

import altair as alt
from vega_datasets import data

source = data.cars()

brush = alt.selection(type='interval', resolve='global')

base = alt.Chart(source).mark_point().encode(
    y='Miles_per_Gallon',
    color=alt.condition(brush, 'Origin', alt.ColorValue('gray')),
).add_selection(
    brush
).properties(
    width=200,
    height=250
)

base.encode(x='Horsepower') | base.encode(x='Acceleration')
