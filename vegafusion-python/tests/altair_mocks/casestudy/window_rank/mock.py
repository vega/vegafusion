# https://altair-viz.github.io/gallery/window_rank.html

import altair as alt
import pandas as pd

source = pd.DataFrame(
    [
        {"team": "Germany", "matchday": 1, "point": 0, "diff": -1},
        {"team": "Germany", "matchday": 2, "point": 3, "diff": 0},
        {"team": "Germany", "matchday": 3, "point": 3, "diff": -2},
        {"team": "Mexico", "matchday": 1, "point": 3, "diff": 1},
        {"team": "Mexico", "matchday": 2, "point": 6, "diff": 2},
        {"team": "Mexico", "matchday": 3, "point": 6, "diff": -1},
        {"team": "South Korea", "matchday": 1, "point": 0, "diff": -1},
        {"team": "South Korea", "matchday": 2, "point": 0, "diff": -2},
        {"team": "South Korea", "matchday": 3, "point": 3, "diff": 0},
        {"team": "Sweden", "matchday": 1, "point": 3, "diff": 1},
        {"team": "Sweden", "matchday": 2, "point": 3, "diff": 0},
        {"team": "Sweden", "matchday": 3, "point": 6, "diff": 3},
    ]
)

color_scale = alt.Scale(
    domain=["Germany", "Mexico", "South Korea", "Sweden"],
    range=["#000000", "#127153", "#C91A3C", "#0C71AB"],
)

alt.Chart(source).mark_line().encode(
    x="matchday:O", y="rank:O", color=alt.Color("team:N", scale=color_scale)
).transform_window(
    rank="rank()",
    sort=[
        alt.SortField("point", order="descending"),
        alt.SortField("diff", order="descending"),
    ],
    groupby=["matchday"],
).properties(title="World Cup 2018: Group F Rankings")
