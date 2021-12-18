# https://altair-viz.github.io/gallery/pie_chart.html

import pandas as pd
import altair as alt

source = pd.DataFrame(
    {"category": ["a", "b", "c", "d", "e", "f"], "value": [4, 6, 10, 3, 7, 8]}
)

base = alt.Chart(source).encode(
    theta=alt.Theta("value:Q", stack=True), color=alt.Color("category:N", legend=None)
)

pie = base.mark_arc(outerRadius=120)
text = base.mark_text(radius=140, size=20).encode(text="category:N")

pie + text
