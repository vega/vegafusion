# https://altair-viz.github.io/gallery/beckers_barley_wrapped_facet.html

import altair as alt
from vega_datasets import data

source = data.barley.url

alt.Chart(source).mark_point().encode(
    alt.X('median(yield):Q', scale=alt.Scale(zero=False)),
    y='variety:O',
    color='year:N',
    facet=alt.Facet('site:O', columns=2),
).properties(
    width=200,
    height=100,
)
