# https://altair-viz.github.io/gallery/bar_chart_with_mean_line.html

import altair as alt
from vega_datasets import data

source = data.wheat()

bar = alt.Chart(source).mark_bar().encode(
    x='year:O',
    y='wheat:Q'
)

rule = alt.Chart(source).mark_rule(color='red').encode(
    y='mean(wheat):Q'
)

(bar + rule).properties(width=600)
