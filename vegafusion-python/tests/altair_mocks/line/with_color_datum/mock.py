# https://altair-viz.github.io/gallery/line_chart_with_color_datum.html

import altair as alt
from vega_datasets import data

source = data.movies()

alt.Chart(source).mark_line().encode(
    x=alt.X("IMDB_Rating", bin=True),
    y=alt.Y(
        alt.repeat("layer"), aggregate="mean", title="Mean of US and Worldwide Gross"
    ),
    color=alt.datum(alt.repeat("layer")),
).repeat(layer=["US_Gross", "Worldwide_Gross"])
