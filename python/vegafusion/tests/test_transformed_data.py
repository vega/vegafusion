from pathlib import Path

import pandas as pd
import pytz
import pytest
from altair.utils.execeval import eval_block
import vegafusion as vf
from vega_datasets import data
import polars as pl
import altair as alt


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
        ("area/layered", 51, ["year", "source", "net_generation"]),
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


@pytest.mark.parametrize(
    "mock_name,expected_lens,all_expected_cols", [
        ("area/horizon_graph", [20, 20], [["x", "y"], ["x", "y", "ny"]]),
        ("bar/and_tick_chart", [7, 7], [["goal", "score_start"], ["project", "goal"]]),
        ("bar/stacked_with_text_overlay", [60, 60], [["site", "sum_yield_start"], ["variety", "sum_yield_end"]]),
        ("bar/with_labels", [52, 52], [["wages", "wheat_start"], ["wheat", "wages"]]),
        ("bar/with_line_at_mean", [52, 1], [["wages", "wheat_start"], ["mean_wheat"]]),
        ("bar/with_line_on_dual_axis", [52, 52], [["wages", "wheat_start"], ["wheat", "wages"]]),
        ("bar/with_rolling_mean", [52, 52], [["wages", "wheat_start"], ["wheat", "wages"]]),
        ("casestudy/co2_concentration", [713, 7, 7], [["year", "decade"], ["scaled_date", "first_date"], ["end"]]),
        ("casestudy/falkensee", [2, 38, 38], [["event", "start"], ["population", "year"], ["year"]]),
        ("casestudy/us_employment", [120, 1, 2], [["construction"], ["president", "end"], ["start"]]),
        ("casestudy/wheat_wages", [52, 52, 52, 52], [["wheat"], ["year_end"], ["year"], ["year"]]),
        ("histogram/with_a_global_mean_overlay", [9, 1], [["bin_maxbins_10_IMDB_Rating_end"], ["mean_IMDB_Rating"]]),
        ("interactive/area-interval_selection", [123, 123], [["price_start"], ["price_end"]]),
        ("interactive/casestudy-seattle_weather_interactive", [1461, 5], [["monthdate_date"], ["__count"]]),
        ("interactive/casestudy-us_population_pyramid_over_time", [19, 38, 19], [["sum_people"], ["people"], ["sum_people_end"]]),
        ("interactive/cross_highlight", [64, 64, 13], [["__count"], ["__count"], ["__count"]]),
        ("interactive/histogram-responsive", [20, 20], [["__count"], ["__count"]]),
        ("interactive/multiline_highlight", [560, 560], [["price"], ["price"]]),
        ("interactive/multiline_tooltip", [300, 300, 300], [["x"], ["y"], ["category"]]),
        ("interactive/scatter-with_linked_table", [392, 19, 19, 19], [["Year"], ["rank"], ["rank"], ["rank"]]),
        ("interactive/scatter-with_minimap", [1461, 1461], [["weather"], ["weather"]]),
        ("interactive/scatter_with_histogram", [100, 10], [["mbin_end"], ["__count"]]),
        ("interactive/scatter_with_layered_histogram", [2, 19], [["mean_height"], ["bin_step_5_age"]]),
        ("interactive/select_detail", [20, 1000], [["mean_y"], ["value"]]),
        ("interactive/select_mark_area", [122, 122], [["sum_count"], ["yearmonth_date"]]),
        ("interactive/selection_histogram", [392, 3], [["Cylinders"], ["__count"]]),
        ("interactive/selection_layer_bar_month", [12, 1], [["mean_precipitation"], ["mean_precipitation"]]),
        ("line/layer_line_color_rule", [560, 5], [["symbol"], ["average_price"]]),
        ("other/bar_chart_with_highlighted_segment", [52, 1, 1], [["wheat_start"], ["baseline"], ["threshold"]]),
        ("other/candlestick_chart", [44, 44], [["ret"], ["signal"]]),
        ("other/errorbars_with_std", [10, 10], [["mean_yield"], ["variety"]]),
        ("other/layered_chart_with_dual_axis", [12, 12], [["average_temp_max"], ["average_temp_max"]]),
        ("other/layered_heatmap_text", [9, 9], [["Origin"], ["num_cars"]]),
        ("other/ranged_dot_plot", [10, 10], [["life_expect"], ["country"]]),
        ("other/scatter_marginal_hist", [34, 150, 27], [["__count"], ["species"], ["__count"]]),
        ("scatter/dot_dash_plot", [400, 392, 398], [["Cylinders"], ["Cylinders"], ["Cylinders"]]),
        ("scatter/with_errorbars", [5, 5], [["ymin"], ["upper_ymin"]]),
        ("scatter/with_labels", [5, 5], [["x"], ["label"]]),
        ("scatter/with_rolling_mean", [1461, 1461], [["precipitation"], ["rolling_mean"]]),
    ]
)
@pytest.mark.parametrize("connection", get_connections())
def test_multi_transformed_data_for_mock(mock_name, expected_lens, all_expected_cols, connection):
    vf.runtime.set_connection(connection)
    mock_path = altair_mocks_dir / mock_name / "mock.py"
    mock_src = mock_path.read_text("utf8")
    chart = eval_block(mock_src)
    dfs = vf.transformed_data(chart)

    for df, expected_len, expected_cols in zip(dfs, expected_lens, all_expected_cols):
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


def test_transformed_data_exclude():
    source = data.wheat()
    bar = alt.Chart(source).mark_bar().encode(x="year:O", y="wheat:Q")
    rule = alt.Chart(source).mark_rule(color="red").encode(y="mean(wheat):Q")
    some_annotation = (
        alt.Chart(name="some_annotation")
        .mark_text(fontWeight="bold")
        .encode(text=alt.value("Just some text"), y=alt.datum(85), x=alt.value(200))
    )

    chart = (bar + rule + some_annotation).properties(width=600)
    datasets = vf.transformed_data(chart, exclude=["some_annotation"])

    assert len(datasets) == 2
    assert len(datasets[0]) == 52
    assert "wheat_start" in datasets[0]
    assert len(datasets[1]) == 1
    assert "mean_wheat" in datasets[1]


@pytest.mark.parametrize("connection", get_connections())
def test_gh_286(connection):
    # https://github.com/hex-inc/vegafusion/issues/286
    vf.runtime.set_connection(connection)
    source = pl.from_pandas(data.seattle_weather())

    chart = alt.Chart(source).mark_bar(
        cornerRadiusTopLeft=3,
        cornerRadiusTopRight=3
    ).encode(
        x='month(date):O',
        y='count():Q',
        color='weather:N'
    )
    transformed = vf.transformed_data(chart)
    assert isinstance(transformed, pl.DataFrame)
    assert len(transformed) == 53


@pytest.mark.parametrize("connection", get_connections())
def test_categorical_columns(connection):
    vf.runtime.set_connection(connection)

    df = pd.DataFrame({
        "a": [0, 1, 2, 3, 4, 5],
        "categorical": pd.Categorical.from_codes([0, 1, 0, 1, 1, 0], ["A", "BB"])
    })

    chart = alt.Chart(df).mark_bar().encode(
        alt.X("categorical:N"), alt.Y("sum(a):Q")
    )
    transformed = vf.transformed_data(chart)
    expected = pd.DataFrame({"categorical": ["A", "BB"], "sum_a": [7, 8]})
    pd.testing.assert_frame_equal(transformed, expected)