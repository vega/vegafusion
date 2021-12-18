# https://altair-viz.github.io/gallery/scatter_with_minimap.html
# with smaller subplots

import altair as alt
from vega_datasets import data

source = data.seattle_weather()

zoom = alt.selection_interval(encodings=["x", "y"])

minimap = (
    alt.Chart(source)
        .mark_point()
        .add_selection(zoom)
        .encode(
        x="date:T",
        y="temp_max:Q",
        color=alt.condition(zoom, "weather", alt.value("lightgray")),
    )
        .properties(
        width=200,
        height=200,
        title="Minimap -- click and drag to zoom in the detail view",
    )
).properties(width=250, height=300)

detail = (
    alt.Chart(source)
        .mark_point()
        .encode(
        x=alt.X(
            "date:T", scale=alt.Scale(domain={"selection": zoom.name, "encoding": "x"})
        ),
        y=alt.Y(
            "temp_max:Q",
            scale=alt.Scale(domain={"selection": zoom.name, "encoding": "y"}),
        ),
        color="weather",
    ).properties(width=250, height=300, title="Seattle weather -- detail view")
)

detail | minimap
