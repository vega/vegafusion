import json
import pathlib
from .renderer import spec_to_mime_bundle


def save_html(
    chart,
    file,
    embed_options=None,
    inline=False,
    full_html=True,
):
    """
    Save an Altair Chart to an HTML file after pre-applying data transformations
    and removing unused columns

    :param chart: alt.Chart
        The Altair chart to save
    :param file: str, pathlib.Path, or file-like object
        The file path to write the HTML file to, or a file-like object to write to
    :param embed_options: dict (default None)
        Dictionary of options to pass to vega-embed
    :param inline: boolean (default False)
        If False (default), the required JavaScript libraries are loaded from a
        CDN location. This results in a smaller file, but an internet connection
        is required to view the resulting HTML file.

        If True, all required JavaScript libraries are inlined.
        This results in a larger file, but no internet connection is required to view.
        inline=True requires Altair 5 and the altair_viewer package
    :param full_html: boolean (default True)
        If True, then a full html page is written. If False, then
        an HTML snippet that can be embedded into an HTML page is written.
    """
    from . import enable

    with enable():
        vegalite_spec = chart.to_dict()

    bundle = spec_to_mime_bundle(
        spec=vegalite_spec,
        mimetype="html",
        embed_options=embed_options,
        html_template="inline" if inline else "standard",
        full_html=full_html,
    )

    html = bundle["text/html"]
    write_file_or_filename(file, html)


def save_vega(
    chart,
    file,
    pretty=True,
):
    """
    Save an Altair Chart to a Vega JSON file after pre-applying data transformations
    and removing unused columns

    :param chart: alt.Chart
        The Altair chart to save
    :param file: str, pathlib.Path, or file-like object
        The file path to write the Vega JSON file to, or a file-like object to write to
    :param pretty: boolean (default True)
        If True, pretty-print the resulting JSON file. If False, write the smallest file possible
    """
    from . import enable

    with enable():
        vegalite_spec = chart.to_dict()

    (bundle, _) = spec_to_mime_bundle(
        spec=vegalite_spec,
        mimetype="vega",
    )

    vega_dict = bundle["application/vnd.vega.v5+json"]
    if pretty:
        vega_str = json.dumps(vega_dict, indent=2)
    else:
        vega_str = json.dumps(vega_dict)

    write_file_or_filename(file, vega_str)


def save_png(
    chart,
    file,
    scale=1,
):
    """
    Save an Altair Chart to a static PNG image file after pre-applying data transformations
    and removing unused columns

    :param chart: alt.Chart
        The Altair chart to save
    :param file: str, pathlib.Path, or file-like object
        The file path to write the PNG file to, or a file-like object to write to
    :param scale: float (default 1)
        Image scale factor
    """
    from . import enable

    with enable():
        vegalite_spec = chart.to_dict()

    bundle = spec_to_mime_bundle(
        spec=vegalite_spec,
        mimetype="png",
        scale=scale,
    )

    png = bundle["image/png"]
    write_file_or_filename(file, png, "wb")


def save_svg(
    chart,
    file,
):
    """
    Save an Altair Chart to a static SVG image file after pre-applying data transformations
    and removing unused columns

    :param chart: alt.Chart
        The Altair chart to save
    :param file: str, pathlib.Path, or file-like object
        The file path to write the SVG file to, or a file-like object to write to
    """
    from . import enable

    with enable():
        vegalite_spec = chart.to_dict()

    bundle = spec_to_mime_bundle(
        spec=vegalite_spec,
        mimetype="svg",
    )

    svg = bundle["image/svg+xml"]
    write_file_or_filename(file, svg, "w")


def write_file_or_filename(file, content, mode="w"):
    """Write content to file, whether file is a string, a pathlib Path or a
    file-like object"""
    if isinstance(file, str) or isinstance(file, pathlib.PurePath):
        with open(file, mode) as f:
            f.write(content)
    else:
        file.write(content)
