# https://altair-viz.github.io/gallery/bar_with_rolling_mean.html

import altair as alt
from vega_datasets import data

source = data.wheat()

bar = alt.Chart(source).mark_bar().encode(
    x='year:O',
    y='wheat:Q'
)

line = alt.Chart(source).mark_line(color='red').transform_window(
    # The field to average
    rolling_mean='mean(wheat)',
    # The number of values before and after the current value to include.
    frame=[-9, 0]
).encode(
    x='year:O',
    y='rolling_mean:Q'
)

(bar + line).properties(width=600)
