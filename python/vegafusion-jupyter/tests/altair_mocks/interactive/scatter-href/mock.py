# https://altair-viz.github.io/gallery/scatter_href.html

import altair as alt
from vega_datasets import data

source = data.cars()

alt.Chart(source).transform_calculate(
    url='https://www.google.com/search?q=' + alt.datum.Name
).mark_point().encode(
    x='Horsepower:Q',
    y='Miles_per_Gallon:Q',
    color='Origin:N',
    href='url:N',
    tooltip=['Name:N', 'url:N']
)
