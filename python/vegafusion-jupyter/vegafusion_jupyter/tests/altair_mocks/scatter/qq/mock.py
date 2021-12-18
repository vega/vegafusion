# https://altair-viz.github.io/gallery/area_chart_gradient.html

import altair as alt
from vega_datasets import data

source = data.normal_2d.url

base = alt.Chart(source).transform_quantile(
    'u',
    step=0.01,
    as_ = ['p', 'v']
).transform_calculate(
    uniform = 'quantileUniform(datum.p)',
    normal = 'quantileNormal(datum.p)'
).mark_point().encode(
    alt.Y('v:Q')
).properties(width=300)

base.encode(x='uniform:Q') | base.encode(x='normal:Q')
