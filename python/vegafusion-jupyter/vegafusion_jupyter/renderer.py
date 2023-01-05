import altair as alt

def vegafusion_renderer(spec, **widget_options):
    """
    Altair renderer that displays charts using a VegaFusionWidget
    """
    from IPython.display import display
    from vegafusion_jupyter import VegaFusionWidget

    # Display widget as a side effect, then return empty string text representation
    # so that Altair doesn't also display a string representation
    widget = VegaFusionWidget(spec, **widget_options)
    display(widget)
    return {'text/plain': ""}

alt.renderers.register('vegafusion-widget', vegafusion_renderer)
