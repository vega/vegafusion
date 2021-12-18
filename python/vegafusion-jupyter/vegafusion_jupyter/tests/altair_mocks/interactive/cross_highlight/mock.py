# https://altair-viz.github.io/gallery/interactive_cross_highlight.html
# - Selection count legend removed to keep from shifting bar chart
#

import altair as alt
from vega_datasets import data

source = data.movies.url

pts = alt.selection(type="single", encodings=['x'])

rect = alt.Chart(data.movies.url).mark_rect().encode(
    alt.X('IMDB_Rating:Q', bin=True),
    alt.Y('Rotten_Tomatoes_Rating:Q', bin=True),
    alt.Color('count()',
              scale=alt.Scale(scheme='greenblue'),
              legend=alt.Legend(title='Total Records')
              )
).properties(
    width=500,
    height=250
)

circ = rect.mark_point().encode(
    alt.ColorValue('grey'),
    alt.Size('count()', legend=None)
).transform_filter(
    pts
).properties(
    width=500,
    height=250
)

bar = alt.Chart(source).mark_bar().encode(
    x='Major_Genre:N',
    y='count()',
    color=alt.condition(pts, alt.ColorValue("steelblue"), alt.ColorValue("grey"))
).properties(
    width=500,
    height=100
).add_selection(pts)

alt.vconcat(
    rect + circ,
    bar
).resolve_legend(
    color="independent",
    size="independent"
)
