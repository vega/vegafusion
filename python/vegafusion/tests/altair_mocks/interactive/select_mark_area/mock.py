import altair as alt
from vega_datasets import data

source = data.unemployment_across_industries.url

base = alt.Chart(source).mark_area(
    color='goldenrod',
    opacity=0.3
).encode(
    x='yearmonth(date):T',
    y='sum(count):Q',
)

brush = alt.selection_interval(encodings=['x'])
background = base.add_params(brush)
selected = base.transform_filter(brush).mark_area(color='goldenrod')

background + selected