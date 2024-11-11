project = 'VegaFusion'
copyright = '2024'
author = 'Jon Mease'

extensions = [
    'myst_parser',
    'sphinx_copybutton',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_design',
]

# Theme settings
html_theme = 'furo'
html_static_path = ['_static']
html_logo = "_static/VegaFusionLogo-Color.svg"
html_favicon = "_static/favicon.ico"

_social_img = "https://vegafusion.io/_static/vegafusion_social.png"
_description = "VegaFusion provides serverside scaling for Vega visualizations"
_title = "VegaFusion"

# MyST settings
myst_enable_extensions = [
    "colon_fence",    # Allow ::: for admonitions
    "deflist",        # Definition lists
    "fieldlist",      # Field lists
    "tasklist",       # Task lists
    "attrs_inline",   # Inline attributes
]

# Add custom CSS
html_css_files = [
    'custom.css',
] 