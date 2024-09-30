import altair as alt
from vega_datasets import data

source = data.stocks()

highlight = alt.selection_point(on='mouseover',
                                fields=['symbol'], nearest=True)

base = alt.Chart(source).encode(
    x='date:T',
    y='price:Q',
    color='symbol:N'
)

points = base.mark_circle().encode(
    opacity=alt.value(0)
).add_params(
    highlight
).properties(
    width=600
)

lines = base.mark_line().encode(
    size=alt.condition(~highlight, alt.value(1), alt.value(3))
)

points + lines