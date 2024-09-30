# https://altair-viz.github.io/gallery/binned_heatmap.html

import altair as alt
from vega_datasets import data

source = data.movies.url

alt.Chart(source).mark_rect().encode(
    alt.X('IMDB_Rating:Q', bin=alt.Bin(maxbins=60)),
    alt.Y('Rotten_Tomatoes_Rating:Q', bin=alt.Bin(maxbins=40)),
    alt.Color('count():Q', scale=alt.Scale(scheme='greenblue'))
)
