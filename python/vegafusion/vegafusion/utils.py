class RendererTransformerEnabler:
    """
    Context manager for enabling a renderer and data
    transformer together
    """
    def __init__(self, renderer_ctx, data_transformer_ctx, repr_str=None):
        self.renderer_ctx = renderer_ctx
        self.data_transformer_ctx = data_transformer_ctx
        self.repr_str = repr_str

    def __enter__(self):
        self.renderer_ctx.__enter__()
        self.data_transformer_ctx.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.renderer_ctx.__exit__(*args, **kwargs)
        self.data_transformer_ctx.__exit__(*args, **kwargs)

    def __repr__(self):
        if self.repr_str:
            return self.repr_str
        else:
            return "vegafusion.utils.RendererTransformerEnabler"
