# https://altair-viz.github.io/gallery/percentage_of_total.html

import altair as alt
import pandas as pd

source = pd.DataFrame(
    {
        "Activity": ["Sleeping", "Eating", "TV", "Work", "Exercise"],
        "Time": [8, 2, 4, 8, 2],
    }
)

alt.Chart(source).transform_joinaggregate(
    TotalTime="sum(Time)",
).transform_calculate(PercentOfTotal="datum.Time / datum.TotalTime").mark_bar().encode(
    alt.X("PercentOfTotal:Q", axis=alt.Axis(format=".0%")), y="Activity:N"
)
