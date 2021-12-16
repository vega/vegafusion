# https://altair-viz.github.io/gallery/stacked_bar_chart_sorted_segments.html

import altair as alt
from vega_datasets import data

source = data.barley()

alt.Chart(source).mark_bar().encode(
    x='sum(yield)',
    y='variety',
    color='site',
    order=alt.Order(
        # Sort the segments of the bars by this field
        'site',
        sort='ascending'
    )
)
