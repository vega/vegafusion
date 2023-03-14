from pathlib import Path
import json
import vegafusion as vf
from vl_convert import vega_to_png

from io import BytesIO
from skimage.io import imread
from skimage.metrics import structural_similarity as ssim
import pytest
here = Path(__file__).parent

spec_dir = here / ".." / ".." / ".." / "vegafusion-runtime" / "tests" / "specs"


def load_test_cases():
    cases = []
    for category in ["vegalite", "custom"]:
        category_dir = spec_dir / category
        for file in category_dir.glob("*.vg.json"):
            cases.append((category, file.name))

    return cases


def maybe_skip(category, name):
    if (category, name) in [
        ("vegalite", "line_mean_month.vg.json"),
        ("vegalite", "errorband_2d_vertical_borders.vg.json"),
        ("vegalite", "area_vertical.vg.json"),
        ("vegalite", "errorbar_2d_vertical_ticks.vg.json"),
        ("vegalite", "time_parse_utc.vg.json"),
        ("vegalite", "layer_candlestick.vg.json"),
        ("vegalite", "layer_dual_axis.vg.json"),
        ("vegalite", "errorband_tooltip.vg.json"),
        ("vegalite", "interactive_seattle_weather.vg.json"),
        ("vegalite", "vconcat_weather.vg.json"),
        ("vegalite", "line_max_year.vg.json"),
        ("vegalite", "bar_month_temporal.vg.json"),
        ("vegalite", "repeat_child_layer.vg.json"),
        ("vegalite", "line_calculate.vg.json"),
        ("vegalite", "stacked_bar_size.vg.json"),
        ("vegalite", "layer_line_errorband_ci.vg.json"),
        ("vegalite", "selection_layer_bar_month.vg.json"),
        ("vegalite", "interactive_query_widgets.vg.json"),
        ("vegalite", "stacked_bar_count_corner_radius_mark.vg.json"),
        ("vegalite", "point_dot_timeunit_color.vg.json"),
        ("vegalite", "stacked_bar_count_corner_radius_mark_x.vg.json"),
        ("vegalite", "line_color_binned.vg.json"),
        ("vegalite", "interactive_point_init.vg.json"),
        ("vegalite", "line_month_center_band.vg.json"),
        ("vegalite", "line_timeunit_transform.vg.json"),
        ("vegalite", "line_concat_facet.vg.json"),
        ("vegalite", "repeat_line_weather.vg.json"),
        ("vegalite", "bar_month_band_config.vg.json"),
        ("vegalite", "layer_precipitation_mean.vg.json"),
        ("vegalite", "layer_bar_month.vg.json"),
        ("vegalite", "errorband_2d_horizontal_color_encoding.vg.json"),
        ("vegalite", "hconcat_weather.vg.json"),
        ("vegalite", "layer_line_co2_concentration.vg.json"),
        ("vegalite", "line_month.vg.json"),
        ("vegalite", "line_mean_year.vg.json"),
        ("vegalite", "stacked_bar_count_corner_radius_stroke.vg.json"),
        ("vegalite", "rect_heatmap_weather.vg.json"),
        ("vegalite", "bar_month_temporal_initial.vg.json"),
        ("vegalite", "bar_month_band.vg.json"),
        ("vegalite", "stacked_area_ordinal.vg.json"),
        ("vegalite", "area_temperature_range.vg.json"),
        ("vegalite", "bar_yearmonth.vg.json"),
        ("vegalite", "bar_yearmonth_custom_format.vg.json"),
        ("vegalite", "stacked_bar_count_corner_radius_config.vg.json"),
        ("vegalite", "layer_line_errorband_2d_horizontal_borders_strokedash.vg.json"),
        ("vegalite", "bar_month.vg.json"),
        ("vegalite", "stacked_bar_weather.vg.json"),
        ("vegalite", "bar_grouped_stacked.vg.json"),
        ("vegalite", "stacked_bar_count.vg.json"),
        ("vegalite", "concat_weather.vg.json"),
        ("custom", "stacked_bar_weather_month.vg.json"),
        ("custom", "interactive_seattle_weather.vg.json"),
        ("custom", "seattle_temps_heatmap.vg.json"),
        ("custom", "stacked_bar_weather_timeunit_parameterize_local.vg.json"),
        ("custom", "selection_layer_bar_month.vg.json"),
        ("custom", "bar_month_temporal_initial_parameterize_local.vg.json"),
        ("custom", "bar_month_temporal_initial_parameterize.vg.json"),
        ("custom", "stacked_bar_weather_timeunit_parameterize.vg.json"),
        ("custom", "interactive_layered_crossfilter.vg.json"),
        ("custom", "layer_precipitation_mean.vg.json"),
        ("custom", "layer_line_co2_concentration.vg.json"),
        ("custom", "rect_heatmap_weather.vg.json"),
        ("custom", "bar_month_temporal_initial.vg.json"),
        ("custom", "ridgeline.vg.json"),
        ("custom", "interactive_average.vg.json"),
    ]:
        # The DataFusion backend follows the Vega/JavaScript idiosyncrasy of parsing
        # datetime strings in the local timezone unless they are of the form YYYY-MM-DD, in which
        # case they are parsed in UTC. The DuckDb connection parses all datetime strings as local
        pytest.skip("Known date parsing timezone mismatch")
    elif (category, name) in [
        ("vegalite", "interactive_multi_line_pivot_tooltip.vg.json"),
        ("vegalite", "interactive_bin_extent_bottom.vg.json"),
        ("vegalite", "interactive_layered_crossfilter_discrete.vg.json"),
        ("vegalite", "interactive_overview_detail.vg.json"),
        ("vegalite", "bar_custom_time_domain.vg.json"),
        ("vegalite", "time_parse_local.vg.json"),
        ("vegalite", "line.vg.json"),
        ("vegalite", "trellis_area_sort_array.vg.json"),
        ("vegalite", "time_output_utc_timeunit.vg.json"),
        ("vegalite", "layer_timeunit_rect.vg.json"),
        ("vegalite", "line_step.vg.json"),
        ("vegalite", "line_overlay.vg.json"),
        ("vegalite", "time_output_utc_scale.vg.json"),
        ("vegalite", "layer_line_mean_point_raw.vg.json"),
        ("vegalite", "joinaggregate_residual_graph.vg.json"),
        ("vegalite", "area_gradient.vg.json"),
        ("vegalite", "interactive_index_chart.vg.json"),
        ("vegalite", "line_color_label.vg.json"),
        ("vegalite", "interactive_line_hover.vg.json"),
        ("vegalite", "layer_line_color_rule.vg.json"),
        ("vegalite", "interactive_multi_line_label.vg.json"),
        ("vegalite", "trellis_line_quarter.vg.json"),
        ("vegalite", "line_color.vg.json"),
        ("vegalite", "line_shape_overlay.vg.json"),
        ("vegalite", "layer_color_legend_left.vg.json"),
        ("vegalite", "line_conditional_axis.vg.json"),
        ("vegalite", "line_quarter_legend.vg.json"),
        ("vegalite", "time_custom_step.vg.json"),
        ("vegalite", "trellis_column_year.vg.json"),
        ("vegalite", "interactive_layered_crossfilter.vg.json"),
        ("vegalite", "line_monotone.vg.json"),
        ("vegalite", "joinaggregate_mean_difference.vg.json"),
        ("vegalite", "layer_line_datum_rule_datetime.vg.json"),
        ("vegalite", "selection_brush_timeunit.vg.json"),
        ("vegalite", "line_strokedash.vg.json"),
        ("vegalite", "interactive_stocks_nearest_index.vg.json"),
        ("vegalite", "line_overlay_stroked.vg.json"),
        ("vegalite", "layer_single_color.vg.json"),
        ("vegalite", "line_domainminmax.vg.json"),
        ("vegalite", "interactive_line_brush_cursor.vg.json"),
        ("vegalite", "rect_lasagna.vg.json"),
        ("vegalite", "line_color_halo.vg.json"),
        ("vegalite", "line_conditional_axis.vg.json"),
        ("vegalite", "repeat_histogram_flights.vg.json"),
        ("vegalite", "interactive_bin_extent.vg.json"),
        ("vegalite", "stacked_area_overlay.vg.json"),
        ("vegalite", "line_detail.vg.json"),
        ("vegalite", "layer_line_datum_rule.vg.json"),
        ("vegalite", "trellis_area.vg.json"),
        ("vegalite", "trail_color.vg.json"),
        ("vegalite", "joinaggregate_mean_difference_by_year.vg.json"),
        ("vegalite", "line_conditional_axis_config.vg.json"),
        ("vegalite", "area_overlay.vg.json"),
        ("custom", "stack_divide_by_zero_error.vg.json"),
        ("custom", "histogram_responsive.vg.json"),
        ("custom", "joinaggregate_movie_rating.vg.json"),
        ("custom", "datetime_scatter.vg.json"),
        ("custom", "rect_lasagna.vg.json"),
        ("custom", "time_boolean_bug.vg.json"),
        ("custom", "sorted_pivot_lines.vg.json"),
        ("custom", "line_color_stocks.vg.json"),
    ]:
        # The DuckDb backend doesn't parse all the timestamp formats that the DataFusion backend does
        # (e.g. 2001/01/10 18:20)
        pytest.skip("Unsupported date format for DuckDB")
    elif (category, name) in [
        ("vegalite", "window_cumulative_running_average.vg.json"),
        ("custom", "cumulative_running_window.vg.json"),
    ]:
        # Duckdb doesn't seem to always support multiple columns that differ only by case. e.g.
        # with top as (SELECT 1 as "year", 2 as "Year")
        #   SELECT "year", "Year" from top
        # ┌───────┬───────┐
        # │ year  │ year  │
        # │ int32 │ int32 │
        # ├───────┼───────┤
        # │     1 │     1 │
        # └───────┴───────┘
        pytest.skip("Duckdb doesn't support multiple columns that differ by case")
    elif (category, name) in [
        ("vegalite", "layer_point_errorbar_ci.vg.json"),
        ("vegalite", "point_offset_random.vg.json"),
        ("vegalite", "sample_scatterplot.vg.json"),
        ("vegalite", "point_ordinal_bin_offset_random.vg.json"),
        ("vegalite", "layer_point_errorbar_2d_horizontal.vg.json"),
        ("vegalite", "layer_point_errorbar_2d_horizontal_ci.vg.json"),
    ]:
        # Non-deterministic (e.g. use of ci0 aggregation function)
        pytest.skip("Non-deterministic specification")


@pytest.mark.duckdb
@pytest.mark.parametrize(
    "category,name", load_test_cases())
def test_it(category, name):

    maybe_skip(category, name)

    # Load spec into dict
    file = spec_dir / category / name
    spec = json.loads(file.read_text("utf8"))

    # Define local timezone
    local_tz = "America/New_York"

    # Pre-transform with DataFusion connection and convert to image
    vf.runtime.set_connection("datafusion")
    (transformed, _) = vf.runtime.pre_transform_spec(spec, local_tz)
    img_datafusion = imread(BytesIO(vega_to_png(transformed)))

    # Pre-transform with DuckDB connection and convert to image
    vf.runtime.set_connection("duckdb")
    (transformed, _) = vf.runtime.pre_transform_spec(spec, local_tz)
    img_duckdb = imread(BytesIO(vega_to_png(transformed)))

    # Compare images
    assert img_datafusion.shape == img_duckdb.shape, "Size mismatch between datafusion and duckdb connections"
    similarity = ssim(img_datafusion, img_duckdb, channel_axis=2)
    print(similarity)
    assert similarity >= 0.999, f"Similarity failed between datafusion and duckdb connections"
