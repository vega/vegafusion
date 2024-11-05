project = 'VegaFusion'
copyright = '2024'
author = 'Jon Mease'

extensions = [
    'myst_parser',
    'sphinx_copybutton',
]

# Theme settings
html_theme = 'furo'
# html_static_path = ['_static']

# MyST settings
myst_enable_extensions = [
    "colon_fence",    # Allow ::: for admonitions
    "deflist",        # Definition lists
    "fieldlist",      # Field lists
    "tasklist",       # Task lists
    "attrs_inline",   # Inline attributes
] 