import altair as alt
from vega_datasets import data
import vegafusion as vf
from io import StringIO, BytesIO
import json


def make_histogram():
    source = data.movies.url
    return alt.Chart(source).mark_bar().encode(
        alt.X("IMDB_Rating:Q", bin=True),
        y='count()',
    )


def test_save_html():
    chart = make_histogram()
    f = StringIO()
    vf.save_html(chart, f)
    html = f.getvalue().strip()
    assert html.startswith("<!DOCTYPE html>")
    assert "https://cdn.jsdelivr.net/npm/vega@5" in html
    assert "https://cdn.jsdelivr.net/npm/vega-embed@6" in html


# We can't test inline HTML until after a new release of Altair Viewer
# def test_save_html_inline():
#     chart = make_histogram()
#     f = StringIO()
#     vf.save_html(chart, f, inline=True)
#     html = f.getvalue().strip()
#     assert html.startswith("<!DOCTYPE html>")
#     assert "<script type=\"text/javascript\">" in html
#     assert "https://cdn.jsdelivr.net/npm/vega@5" not in html
#     assert "https://cdn.jsdelivr.net/npm/vega-embed@6" not in html


def test_save_vega():
    chart = make_histogram()
    f = StringIO()
    vf.save_vega(chart, f)
    vega = json.loads(f.getvalue())
    assert vega["$schema"] == "https://vega.github.io/schema/vega/v5.json"


def test_save_svg():
    chart = make_histogram()
    f = StringIO()
    vf.save_svg(chart, f)
    svg = f.getvalue()
    assert svg.startswith("<svg")


def test_save_png():
    chart = make_histogram()
    f = BytesIO()
    vf.save_png(chart, f)
    png = f.getvalue()
    assert png.startswith(b'\x89PNG')
