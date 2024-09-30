# https://altair-viz.github.io/gallery/image_tooltip.html

import altair as alt
import pandas as pd

source = pd.DataFrame.from_records(
    [{'a': 1, 'b': 1, 'image': 'https://altair-viz.github.io/_static/altair-logo-light.png'},
     {'a': 2, 'b': 2, 'image': 'https://avatars.githubusercontent.com/u/11796929?s=200&v=4'}]
)
alt.Chart(source).mark_circle(size=200).encode(
    x='a',
    y='b',
    tooltip=['image']  # Must be a list for the image to render
)
