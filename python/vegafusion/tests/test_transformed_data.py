from pathlib import Path

import pandas as pd
import pytz
import pytest
from altair.utils.execeval import eval_block
import vegafusion as vf


here = Path(__file__).parent
altair_mocks_dir = here / "altair_mocks"


def get_connections():
    connections = ["datafusion"]
    try:
        import duckdb
        connections.append("duckdb")
    except ImportError:
        pass

    return connections


@pytest.mark.parametrize(
    "mock_name,expected_len,expected_cols", [
        ("area/cumulative_count", 3201, ["Running_Time_min", "cumulative_count"]),
        ("area/gradient", 68, ["symbol", "date", "price"]),
        ("area/normalized_stacked", 51, ["year", "source", "net_generation_start", "net_generation_end"]),
        ("area/streamgraph", 1708, ["series", "yearmonth_date", "sum_count_start", "sum_count_end"]),
        ("area/trellis", 51, ["year", "source", "net_generation_start", "net_generation_end"]),
        ("area/trellis_sort_array", 492, ["symbol", "date", "row_symbol_sort_index", "price_start", "price_end"]),
        ("bar/diverging_stacked", 40, ["question", "percentage", "percentage_start", "percentage_end"]),
        ("bar/grouped", 12, ["year", "site", "sum_yield"]),
        ("bar/horizontal", 52, ["year", "wheat", "wages"]),
        ("bar/horizontal_grouped", 12, ["year", "site", "sum_yield"]),
        ("bar/horizontal_stacked", 60, ["site", "variety", "sum_yield_start", "sum_yield_end"]),
        ("bar/layered", 51, ["year", "source", "net_generation"]),
        ("bar/normalized_stacked", 60, ["site", "variety", "sum_yield_start", "sum_yield_end"]),
        ("bar/percentage_of_total", 5, ["Activity", "Time", "TotalTime", "PercentOfTotal"]),
        ("bar/sorted", 6, ["site", "sum_yield"]),
        ("bar/stacked", 60, ["site", "variety", "sum_yield_start", "sum_yield_end"]),
        ("bar/stacked_with_sorted_segments", 60, ["site", "variety", "sum_yield_start", "sum_yield_end"]),
        ("bar/trellis_compact", 27, ["a", "b", "c", "p"]),
        ("bar/trellis_stacked", 120, ["yield", "variety", "year", "yield_start", "yield_end"]),
        ("bar/with_highlighted_bar", 52, ["year", "wheat", "wages"]),
        ("bar/with_negative_values", 120, ["month", "nonfarm_change"]),
        ("bar/with_rounded_edges", 53, ["weather", "month_date", "__count_start", "__count_end"]),
        ("casestudy/anscombe_plot", 44, ["Series", "X", "Y"]),
        ("casestudy/beckers_barley_trellis_plot", 120, ["yield", "variety", "year", "site"]),
        ("casestudy/gapminder_bubble_plot", 187, ["country", "health", "population"]),
        ("casestudy/iowa_electricity", 51, ["year", "net_generation_start", "net_generation_end"]),
        ("casestudy/isotype", 37, ["country", "animal", "x"]),
        ("casestudy/natural_disasters", 686, ["Entity", "Year", "Deaths"]),
        ("casestudy/top_k_items", 9, ["Title", "IMDB_Rating_start", "IMDB_Rating_end"]),
        ("casestudy/top_k_letters", 9, ["letters", "count", "rank"]),
        ("casestudy/top_k_with_others", 10, ["ranked_director", "mean_aggregate_gross"]),
        ("casestudy/us_population_over_time_facet", 285, ["age", "year", "sum_people"]),
        ("casestudy/window_rank", 12, ["team", "matchday", "rank"]),
        ("circular/donut", 6, ["category", "value_start", "value_end"]),
        ("circular/pie", 6, ["category", "value_start", "value_end"]),
        ("histogram/trellis", 20, ["Origin", "__count", "bin_maxbins_10_Horsepower_end"]),
        ("histogram/layered", 113, ["Experiment", "__count", "bin_maxbins_100_Measurement"]),
        ("interactive/brush", 392, ["Name", "Cylinders", "Origin"]),
        ("interactive/casestudy-us_population_over_time", 38, ["year", "age", "sex", "people"]),
        ("interactive/casestudy-weather_heatmap", 365, ["monthdate_date", "date_date", "max_temp"]),
        ("interactive/legend", 1708, ["yearmonth_date", "sum_count_start", "sum_count_end"]),
        ("interactive/other-image_tooltip", 2, ["a", "b", "image"]),
        ("interactive/scatter-href", 392, ["Name", "Horsepower", "url"]),
        ("interactive/scatter_plot", 392, ["Name", "Horsepower", "Year"]),
        ("line/bump_chart", 100, ["symbol", "rank", "yearmonth_date_end"]),
        ("line/filled_step_chart", 68, ["symbol", "date", "price"]),
        ("line/multi_series", 560, ["symbol", "date", "price"]),
        ("line/percent_axis", 30, ["job", "sex", "year", "count", "perc"]),
        ("line/slope_graph", 12, ["site", "year", "median_yield"]),
        ("line/step_chart", 68, ["symbol", "date", "price"]),
        ("line/with_cumsum", 52, ["year", "wheat", "cumulative_wheat"]),
        ("line/with_generator", 256, ["x", "sin", "cos", "key", "value"]),
        ("line/with_logarithmic_scale", 15, ["year", "sum_people"]),
        ("line/with_points", 100, ["x", "f(x)"]),
        ("other/beckers_barley_wrapped_facet", 120, ["variety", "site", "median_yield"]),
        ("other/binned_heatmap", 378, ["__count", "bin_maxbins_60_IMDB_Rating_end"]),
        ("other/comet_chart", 120, ["variety", "1932", "delta"]),
        ("other/gantt_chart", 3, ["task", "start", "end"]),
        ("other/hexbins", 84, ["month_date", "xFeaturePos", "mean_temp_max"]),
        ("other/isotype_grid", 100, ["id", "row", "col"]),
        ("other/multiple_marks", 560, ["symbol", "date", "price"]),
        ("other/stem_and_leaf", 100, ["samples", "stem", "leaf", "position"]),
        ("other/wilkinson_dot_plot", 21, ["data", "id"]),
        ("other/parallel_coordinates", 600, ["sepalWidth", "index", "key", "value"]),
        ("other/normed_parallel_coordinates", 600, ["sepalWidth", "minmax_value", "mid"]),
        ("other/ridgeline_plot", 108, ["Month", "mean_temp", "value"]),
        ("scatter/binned", 64, ["__count", "bin_maxbins_10_Rotten_Tomatoes_Rating_end"]),
        ("scatter/bubble_plot", 392, ["Name", "Cylinders", "Origin"]),
        ("scatter/connected", 55, ["side", "year", "miles", "gas"]),
        ("scatter/multifeature", 150, ["sepalLength", "petalLength", "species"]),
        ("scatter/table_bubble_plot_github", 168, ["hours_time", "day_time", "sum_count"]),
        ("scatter/trellis", 392, ["Name", "Cylinders", "Year"]),
        ("simple/bar_chart", 9, ["a", "b"]),
        ("simple/heatmap", 100, ["x", "y", "z"]),
        ("simple/line_chart", 100, ["x", "f(x)"]),
        ("simple/scatter_tooltips", 392, ["Name", "Cylinders", "Year"]),
        ("simple/stacked_bar_chart", 51, ["year", "source", "net_generation_end"]),
        ("simple/strip_chart", 400, ["Name", "Cylinders", "Origin"]),
    ]
)
@pytest.mark.parametrize("connection", get_connections())
def test_transformed_data_for_mock(mock_name, expected_len, expected_cols, connection):
    vf.runtime.set_connection(connection)

    mock_path = altair_mocks_dir / mock_name / "mock.py"
    mock_src = mock_path.read_text("utf8")
    chart = eval_block(mock_src)
    df = vf.transformed_data(chart)

    # Check that a DataFrame was returned
    assert isinstance(df, pd.DataFrame)

    # Check that the expected columns are present
    assert set(expected_cols).issubset(set(df.columns))

    # Check that datetime columns have local timezone
    for dtype in df.dtypes.values:
        if dtype.kind == "M":
            assert dtype.tz == pytz.timezone(vf.get_local_tz())

    # Check expected length
    assert len(df) == expected_len
