# https://altair-viz.github.io/gallery/select_mark_area.html

import altair as alt
from vega_datasets import data

source = data.unemployment_across_industries.url

base = alt.Chart(source).mark_area(
    color='goldenrod',
    opacity=0.3
).encode(
    x='yearmonth(date):T',
    y='sum(count):Q',
)

brush = alt.selection_interval(encodings=['x'],empty='all')
background = base.add_selection(brush)
selected = base.transform_filter(brush).mark_area(color='goldenrod')

background + selected
