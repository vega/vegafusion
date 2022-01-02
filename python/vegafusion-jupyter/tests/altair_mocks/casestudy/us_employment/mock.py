# https://altair-viz.github.io/gallery/us_employment.html

import altair as alt
import pandas as pd
from vega_datasets import data

source = data.us_employment()
presidents = pd.DataFrame([
    {
        "start": "2006-01-01",
        "end": "2009-01-19",
        "president": "Bush"
    },
    {
        "start": "2009-01-20",
        "end": "2015-12-31",
        "president": "Obama"
    }
])

bars = alt.Chart(
    source,
    title="The U.S. employment crash during the Great Recession"
).mark_bar().encode(
    x=alt.X("month:T", title=""),
    y=alt.Y("nonfarm_change:Q", title="Change in non-farm employment (in thousands)"),
    color=alt.condition(
        alt.datum.nonfarm_change > 0,
        alt.value("steelblue"),
        alt.value("orange")
    )
)

rule = alt.Chart(presidents).mark_rule(
    color="black",
    strokeWidth=2
).encode(
    x='end:T'
).transform_filter(alt.datum.president == "Bush")

text = alt.Chart(presidents).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=-135,
    size=11
).encode(
    x='start:T',
    x2='end:T',
    text='president',
    color=alt.value('#000000')
)

(bars + rule + text).properties(width=600)
