# https://altair-viz.github.io/gallery/histogram_with_a_global_mean_overlay.html

import altair as alt
from vega_datasets import data

source = data.movies.url

base = alt.Chart(source)

bar = base.mark_bar().encode(
    x=alt.X('IMDB_Rating:Q', bin=True, axis=None),
    y='count()'
)

rule = base.mark_rule(color='red').encode(
    x='mean(IMDB_Rating):Q',
    size=alt.value(5)
)

bar + rule