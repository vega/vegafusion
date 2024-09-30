# https://altair-viz.github.io/gallery/weather_heatmap.html

import altair as alt
from vega_datasets import data

# Since the data is more than 5,000 rows we'll import it from a URL
source = "https://raw.githubusercontent.com/vega/vega-datasets/v1.31.1/data/seattle-temps.csv"

alt.Chart(
    source,
    title="2010 Daily High Temperature (F) in Seattle, WA"
).mark_rect().encode(
    x='date(date):O',
    y='month(date):O',
    color=alt.Color('max(temp):Q', scale=alt.Scale(scheme="inferno")),
    tooltip=[
        alt.Tooltip('monthdate(date):T', title='Date'),
        alt.Tooltip('max(temp):Q', title='Max Temp')
    ]
).properties(width=550)
