# https://altair-viz.github.io/gallery/boxplot.html

import altair as alt
from vega_datasets import data

source = data.population.url

alt.Chart(source).mark_boxplot(extent='min-max').encode(
    x='age:O',
    y='people:Q'
)
