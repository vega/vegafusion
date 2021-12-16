# https://altair-viz.github.io/gallery/line_chart_with_points.html

import altair as alt
import numpy as np
import pandas as pd

x = np.arange(100)
source = pd.DataFrame({
    'x': x,
    'f(x)': np.sin(x / 5)
})

alt.Chart(source).mark_line(
    point=alt.OverlayMarkDef(color="red")
).encode(
    x='x',
    y='f(x)'
)
