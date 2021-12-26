# https://altair-viz.github.io/gallery/errorbars_with_ci.html
# Padding updates to make size deterministic even when data is not

import altair as alt
from vega_datasets import data

source = data.barley()

error_bars = alt.Chart(source).mark_errorbar(extent='ci').encode(
    x=alt.X('yield:Q', scale=alt.Scale(zero=False)),
    y=alt.Y('variety:N')
)

points = alt.Chart(source).mark_point(filled=True, color='black').encode(
    x=alt.X('yield:Q', aggregate='mean'),
    y=alt.Y('variety:N'),
)

(error_bars + points).properties(
    padding={"left": 50, "top": 5, "right": 50, "bottom": 5}
)
