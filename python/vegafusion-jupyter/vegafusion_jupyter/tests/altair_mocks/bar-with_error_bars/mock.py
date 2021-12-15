# https://altair-viz.github.io/gallery/grouped_bar_chart_with_error_bars.html

import altair as alt
from vega_datasets import data

source = data.barley()

bars = alt.Chart().mark_bar().encode(
    x='year:O',
    y=alt.Y('mean(yield):Q', title='Mean Yield'),
    color='year:N',
)

error_bars = alt.Chart().mark_errorbar(extent='ci').encode(
    x='year:O',
    y='yield:Q'
)

alt.layer(bars, error_bars, data=source).facet(
    column='site:N'
)