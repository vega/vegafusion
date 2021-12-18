# https://altair-viz.github.io/gallery/us_population_over_time_facet.html

import altair as alt
from vega_datasets import data

source = data.population.url

alt.Chart(source).mark_area().encode(
    x='age:O',
    y=alt.Y(
        'sum(people):Q',
        title='Population',
        axis=alt.Axis(format='~s')
    ),
    facet=alt.Facet('year:O', columns=5),
).properties(
    title='US Age Distribution By Year',
    width=80,
    height=80
)
