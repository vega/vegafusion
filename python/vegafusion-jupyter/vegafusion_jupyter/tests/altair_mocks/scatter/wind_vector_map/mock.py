# https://altair-viz.github.io/gallery/wind_vector_map.html

import altair as alt
from vega_datasets import data

source = data.windvectors()

alt.Chart(source).mark_point(shape="wedge", filled=True).encode(
    latitude="latitude",
    longitude="longitude",
    color=alt.Color(
        "dir", scale=alt.Scale(domain=[0, 360], scheme="rainbow"), legend=None
    ),
    angle=alt.Angle("dir", scale=alt.Scale(domain=[0, 360], range=[180, 540])),
    size=alt.Size("speed", scale=alt.Scale(rangeMax=500)),
).project("equalEarth")
