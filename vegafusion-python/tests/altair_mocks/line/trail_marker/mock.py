# https://altair-viz.github.io/gallery/trail_marker.html
# With year column converted to a datetime64. VegaFusion doesn't need this, but the altair case isn't handling
# an integer year column correctly.

import altair as alt
from vega_datasets import data

source = data.wheat()
source["year"].astype("datetime64[ms]")

chart = alt.Chart(source).mark_trail().encode(x="year:T", y="wheat:Q", size="wheat:Q")
chart