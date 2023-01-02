import altair as alt
import vegafusion as vf


def test_mime_enabler_context_manager():
    alt.data_transformers.enable("json")
    alt.renderers.enable("mimetype")

    assert alt.data_transformers.active == "json"
    assert alt.renderers.active == "mimetype"

    with vf.disable():
        assert alt.data_transformers.active == "default"
        assert alt.renderers.active == "default"

    assert alt.data_transformers.active == "json"
    assert alt.renderers.active == "mimetype"

    ctx = vf.disable()
    assert alt.data_transformers.active == "default"
    assert alt.renderers.active == "default"
    assert repr(ctx) == "vegafusion.disable()"

    with vf.enable():
        assert alt.data_transformers.active == "vegafusion-inline"
        assert alt.renderers.active == "vegafusion-mime"

    assert alt.data_transformers.active == "default"
    assert alt.renderers.active == "default"

    ctx = vf.enable()
    assert alt.data_transformers.active == "vegafusion-inline"
    assert alt.renderers.active == "vegafusion-mime"
    assert repr(ctx) == "vegafusion.enable(mimetype='html', row_limit=10000, embed_options=None)"
