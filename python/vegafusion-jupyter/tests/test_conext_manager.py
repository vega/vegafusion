import altair as alt
import vegafusion as vf


def test_enabler_context_manager():
    alt.data_transformers.enable("json")
    alt.renderers.enable("mimetype")

    assert alt.data_transformers.active == "json"
    assert alt.renderers.active == "mimetype"

    with vf.enable_widget():
        assert alt.data_transformers.active == "vegafusion-feather"
        assert alt.renderers.active == "vegafusion-widget"

    assert alt.data_transformers.active == "json"
    assert alt.renderers.active == "mimetype"

    ctx = vf.enable_widget()
    assert alt.data_transformers.active == "vegafusion-feather"
    assert alt.renderers.active == "vegafusion-widget"
    assert repr(ctx) == "vegafusion.enable_widget()"


def test_enabler_context_manager_preserves_options():
    # No options
    with vf.enable_widget():
        assert alt.data_transformers.active == "vegafusion-feather"
        assert alt.data_transformers.options == {"data_dir": "_vegafusion_data"}

        assert alt.renderers.active == "vegafusion-widget"
        assert alt.renderers.options == {
            'debounce_max_wait': 60, 'debounce_wait': 30, 'download_source_link': None
        }

        # Check that Widget uses default options
        widget = vf.jupyter.VegaFusionWidget()
        assert widget.debounce_max_wait == 60

    # Options in the enable call
    with vf.enable_widget(debounce_max_wait=200, data_dir="my_data_dir"):
        assert alt.data_transformers.active == "vegafusion-feather"
        assert alt.data_transformers.options == {"data_dir": "my_data_dir"}

        assert alt.renderers.active == "vegafusion-widget"
        assert alt.renderers.options == {
            'debounce_max_wait': 200, 'debounce_wait': 30, 'download_source_link': None
        }

        # Check that widget picks up options from context manager
        widget = vf.jupyter.VegaFusionWidget()
        assert widget.debounce_max_wait == 200
