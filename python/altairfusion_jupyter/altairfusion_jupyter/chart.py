import altair as alt
from .base_chart import BaseChart, mark_methods, transform_methods


class Chart(BaseChart):
    # Type of Altair object being wrapped
    _altair_wrap_class = alt.Chart


class LayerChart(BaseChart):
    # Type of Altair object being wrapped
    _altair_wrap_class = alt.LayerChart


class RepeatChart(BaseChart):
    # Type of Altair object being wrapped
    _altair_wrap_class = alt.RepeatChart


class ConcatChart(BaseChart):
    # Type of Altair object being wrapped
    _altair_wrap_class = alt.ConcatChart


class HConcatChart(BaseChart):
    # Type of Altair object being wrapped
    _altair_wrap_class = alt.HConcatChart


class VConcatChart(BaseChart):
    # Type of Altair object being wrapped
    _altair_wrap_class = alt.VConcatChart
