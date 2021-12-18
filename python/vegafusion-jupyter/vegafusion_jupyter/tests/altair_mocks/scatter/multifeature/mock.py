# https://altair-viz.github.io/gallery/multifeature_scatter_plot.html

import altair as alt
from vega_datasets import data

source = data.iris()

alt.Chart(source).mark_circle().encode(
    alt.X('sepalLength', scale=alt.Scale(zero=False)),
    alt.Y('sepalWidth', scale=alt.Scale(zero=False, padding=1)),
    color='species',
    size='petalWidth'
)
