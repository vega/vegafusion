# https://altair-viz.github.io/gallery/top_k_with_others.html

import altair as alt
from vega_datasets import data

source = data.movies.url

alt.Chart(source).mark_bar().encode(
    x=alt.X("aggregate_gross:Q", aggregate="mean", title=None),
    y=alt.Y(
        "ranked_director:N",
        sort=alt.Sort(op="mean", field="aggregate_gross", order="descending"),
        title=None,
    ),
).transform_aggregate(
    aggregate_gross='mean(Worldwide_Gross)',
    groupby=["Director"],
).transform_window(
    rank='row_number()',
    sort=[alt.SortField("aggregate_gross", order="descending")],
).transform_calculate(
    ranked_director="datum.rank < 10 ? datum.Director : 'All Others'"
).properties(
    title="Top Directors by Average Worldwide Gross",
)
