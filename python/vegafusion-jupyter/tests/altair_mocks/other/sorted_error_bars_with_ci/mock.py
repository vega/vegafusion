# https://altair-viz.github.io/gallery/sorted_error_bars_with_ci.html
# Padding updates to make size deterministic even when data is not

import altair as alt
from vega_datasets import data

source = data.barley()

points = alt.Chart(source).mark_point(
    filled=True,
    color='black'
).encode(
    x=alt.X('mean(yield)', title='Barley Yield'),
    y=alt.Y(
        'variety',
        sort=alt.EncodingSortField(
            field='yield',
            op='mean',
            order='descending'
        )
    )
).properties(
    width=400,
    height=250
)

error_bars = points.mark_rule().encode(
    x='ci0(yield)',
    x2='ci1(yield)',
)

(points + error_bars).properties(
    padding={"left": 50, "top": 5, "right": 50, "bottom": 5}
)
