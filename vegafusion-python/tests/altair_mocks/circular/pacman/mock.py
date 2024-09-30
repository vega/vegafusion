# https://altair-viz.github.io/gallery/pacman_chart.html

import numpy as np
import altair as alt

alt.Chart().mark_arc(color="gold").encode(
    theta=alt.datum((5 / 8) * np.pi, scale=None),
    theta2=alt.datum((19 / 8) * np.pi),
    radius=alt.datum(100, scale=None),
)
