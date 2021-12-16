# https://altair-viz.github.io/gallery/line_chart_with_datum.html

import altair as alt
from vega_datasets import data

source = data.stocks()

lines = (
    alt.Chart(source)
        .mark_line()
        .encode(x="date", y="price", color="symbol")
)

xrule = (
    alt.Chart()
        .mark_rule(color="cyan", strokeWidth=2)
        .encode(x=alt.datum(alt.DateTime(year=2006, month="November")))
)

yrule = (
    alt.Chart().mark_rule(strokeDash=[12, 6], size=2).encode(y=alt.datum(350))
)


lines + yrule + xrule