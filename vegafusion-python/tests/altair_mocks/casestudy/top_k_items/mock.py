# https://altair-viz.github.io/gallery/top_k_items.html

import altair as alt
from vega_datasets import data

source = data.movies.url

# Top 10 movies by IMBD rating
alt.Chart(
    source,
).mark_bar().encode(
    x=alt.X('Title:N', sort='-y'),
    y=alt.Y('IMDB_Rating:Q'),
    color=alt.Color('IMDB_Rating:Q')

).transform_window(
    rank='rank(IMDB_Rating)',
    sort=[
        alt.SortField('IMDB_Rating', order='descending'),
        alt.SortField('Title', order='ascending'),
    ]
).transform_filter(
    (alt.datum.rank < 10)
)
