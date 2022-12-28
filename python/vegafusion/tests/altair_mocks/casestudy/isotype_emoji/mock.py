# https://altair-viz.github.io/gallery/isotype_emoji.html

import altair as alt
import pandas as pd

source = pd.DataFrame([
    {'country': 'Great Britain', 'animal': 'cattle'},
    {'country': 'Great Britain', 'animal': 'cattle'},
    {'country': 'Great Britain', 'animal': 'cattle'},
    {'country': 'Great Britain', 'animal': 'pigs'},
    {'country': 'Great Britain', 'animal': 'pigs'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'Great Britain', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'cattle'},
    {'country': 'United States', 'animal': 'pigs'},
    {'country': 'United States', 'animal': 'pigs'},
    {'country': 'United States', 'animal': 'pigs'},
    {'country': 'United States', 'animal': 'pigs'},
    {'country': 'United States', 'animal': 'pigs'},
    {'country': 'United States', 'animal': 'pigs'},
    {'country': 'United States', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'sheep'},
    {'country': 'United States', 'animal': 'sheep'}
])


alt.Chart(source).mark_text(size=45, baseline='middle').encode(
    alt.X('x:O', axis=None),
    alt.Y('animal:O', axis=None),
    alt.Row('country:N', header=alt.Header(title='')),
    alt.Text('emoji:N')
).transform_calculate(
    emoji="{'cattle': '🐄', 'pigs': '🐖', 'sheep': '🐏'}[datum.animal]"
).transform_window(
    x='rank()',
    groupby=['country', 'animal']
).properties(width=550, height=140)
