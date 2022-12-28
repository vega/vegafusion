# https://altair-viz.github.io/gallery/layer_line_color_rule.html

import altair as alt
from vega_datasets import data

source = data.stocks()

base = alt.Chart(source).properties(width=550)

line = base.mark_line().encode(
    x='date',
    y='price',
    color='symbol'
)

rule = base.mark_rule().encode(
    y='average(price)',
    color='symbol',
    size=alt.value(2)
)

line + rule
