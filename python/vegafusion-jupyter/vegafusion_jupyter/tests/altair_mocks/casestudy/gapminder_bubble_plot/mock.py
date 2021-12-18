# https://altair-viz.github.io/gallery/gapminder_bubble_plot.html

import altair as alt
from vega_datasets import data

source = data.gapminder_health_income.url

alt.Chart(source).mark_circle().encode(
    alt.X('income:Q', scale=alt.Scale(type='log')),
    alt.Y('health:Q', scale=alt.Scale(zero=False)),
    size='population:Q'
)
