# https://altair-viz.github.io/gallery/beckers_barley_trellis_plot.html

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source, title="The Morris Mistake").mark_point().encode(
    alt.X(
        'yield:Q',
        title="Barley Yield (bushels/acre)",
        scale=alt.Scale(zero=False),
        axis=alt.Axis(grid=False)
    ),
    alt.Y(
        'variety:N',
        title="",
        sort='-x',
        axis=alt.Axis(grid=True)
    ),
    color=alt.Color('year:N', legend=alt.Legend(title="Year")),
    facet=alt.Facet(
        'site:N',
        columns=2,
        title="",
        sort=alt.EncodingSortField(field='yield', op='sum', order='descending')
    ),
).properties(
    width=200,
    height=100,
).configure_view(stroke="transparent")
