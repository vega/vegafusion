# https://altair-viz.github.io/gallery/scatter_with_loess.html

import altair as alt
import pandas as pd
import numpy as np

np.random.seed(1)

source = pd.DataFrame({
    'x': np.arange(100),
    'A': np.random.randn(100).cumsum(),
    'B': np.random.randn(100).cumsum(),
    'C': np.random.randn(100).cumsum(),
})

base = alt.Chart(source).mark_circle(opacity=0.5).transform_fold(
    fold=['A', 'B', 'C'],
    as_=['category', 'y']
).encode(
    alt.X('x:Q'),
    alt.Y('y:Q'),
    alt.Color('category:N')
)

base + base.transform_loess('x', 'y', groupby=['category']).mark_line(size=4)
