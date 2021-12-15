# https://altair-viz.github.io/gallery/bar_chart_with_negatives.html

import altair as alt
from vega_datasets import data

source = data.us_employment()

alt.Chart(source).mark_bar().encode(
    x="month:T",
    y="nonfarm_change:Q",
    color=alt.condition(
        alt.datum.nonfarm_change > 0,
        alt.value("steelblue"),  # The positive color
        alt.value("orange")  # The negative color
    )
).properties(width=600)