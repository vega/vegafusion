# https://altair-viz.github.io/gallery/layered_bar_chart.html
# With explicit temporal encoding
import altair as alt
from vega_datasets import data

source = data.iowa_electricity()

alt.Chart(source).mark_bar(opacity=0.7).encode(
    x='year:T',
    y=alt.Y('net_generation:Q', stack=None),
    color="source",
)
