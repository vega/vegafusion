project = 'VegaFusion'
copyright = '2024'
author = 'Jon Mease'

extensions = [
    'myst_parser',
    'sphinx_copybutton',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_design',
    'ablog',
    'sphinx.ext.intersphinx',
]

# Theme settings
html_theme = "pydata_sphinx_theme"
html_static_path = ['_static']
html_logo = "_static/VegaFusionLogo-Color.svg"
html_favicon = "_static/favicon.ico"
html_theme_options = {
    "icon_links": [
        {
            "name": "Twitter",
            "url": "https://twitter.com/vegafusion_io",
            "icon": "fa-brands fa-twitter",
        },
        {
            "name": "GitHub",
            "url": "https://github.com/vega/vegafusion",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/vegafusion/",
            "icon": "fa-custom fa-pypi",
        }
    ],
}
# Add custom CSS
html_css_files = [
    'custom.css',
] 
html_js_files = ["custom-icon.js"]

_social_img = "https://vegafusion.io/_static/vegafusion_social.png"
_description = "VegaFusion provides serverside scaling for Vega visualizations"
_title = "VegaFusion"

# -- Blog configuration ------------------------------------------------------
blog_baseurl = "https://vegafusion.io"  # Replace with your actual base URL
blog_post_pattern = "posts/*/*"
blog_path = "blog"
blog_title = "VegaFusion Blog"
templates_path = ['_templates']

# MyST settings
myst_enable_extensions = [
    "colon_fence",    # Allow ::: for admonitions
    "deflist",        # Definition lists
    "fieldlist",      # Field lists
    "tasklist",       # Task lists
    "attrs_inline",   # Inline attributes
]

