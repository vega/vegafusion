# https://altair-viz.github.io/gallery/cumulative_count_chart.html

import altair as alt
from vega_datasets import data

source = data.movies.url

alt.Chart(source).transform_window(
    cumulative_count="count()",
    sort=[{"field": "IMDB_Rating"}],
).mark_area().encode(
    x="IMDB_Rating:Q",
    y="cumulative_count:Q"
)
