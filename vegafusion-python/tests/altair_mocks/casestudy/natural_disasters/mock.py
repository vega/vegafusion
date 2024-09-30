# https://altair-viz.github.io/gallery/natural_disasters.html

import altair as alt
from vega_datasets import data

source = "https://raw.githubusercontent.com/vega/vega-datasets/main/data/disasters.csv"

alt.Chart(source).mark_circle(
    opacity=0.8,
    stroke='black',
    strokeWidth=1
).encode(
    alt.X('Year:O', axis=alt.Axis(labelAngle=0)),
    alt.Y('Entity:N'),
    alt.Size('Deaths:Q',
             scale=alt.Scale(range=[0, 4000]),
             legend=alt.Legend(title='Annual Global Deaths')
             ),
    alt.Color('Entity:N', legend=None)
).properties(
    width=300,
    height=300
).transform_filter(
    alt.datum.Entity != 'All natural disasters'
)

