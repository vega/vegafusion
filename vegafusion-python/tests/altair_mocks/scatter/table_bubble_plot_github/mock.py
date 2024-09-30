# https://altair-viz.github.io/gallery/table_bubble_plot_github.html

import altair as alt
from vega_datasets import data

source = "https://raw.githubusercontent.com/vega/vega-datasets/v1.31.1/data/github.csv"

alt.Chart(source).mark_circle().encode(
    x='hours(time):O',
    y='day(time):O',
    size='sum(count):Q'
)
