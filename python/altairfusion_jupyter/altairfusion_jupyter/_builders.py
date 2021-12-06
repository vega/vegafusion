import altair as _alt
from altairfusion_jupyter.meta import altair_builder_fn as _altair_builder_fn

selection = _altair_builder_fn(_alt.selection)
selection_single = _altair_builder_fn(_alt.selection_single)
selection_multi = _altair_builder_fn(_alt.selection_multi)
selection_interval = _altair_builder_fn(_alt.selection_interval)

binding = _altair_builder_fn(_alt.binding)
binding_select = _altair_builder_fn(_alt.binding_select)
binding_checkbox = _altair_builder_fn(_alt.binding_checkbox)
binding_radio = _altair_builder_fn(_alt.binding_radio)
binding_range = _altair_builder_fn(_alt.binding_range)

layer = _altair_builder_fn(_alt.layer)
repeat = _altair_builder_fn(_alt.repeat)