# https://altair-viz.github.io/gallery/wilkinson-dot-plot.html

import altair as alt
import pandas as pd

source = pd.DataFrame(
    {"data": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4]}
)

alt.Chart(source).mark_circle(opacity=1).transform_window(
    id="rank()", groupby=["data"]
).encode(alt.X("data:O"), alt.Y("id:O", axis=None, sort="descending")).properties(
    height=100
)
