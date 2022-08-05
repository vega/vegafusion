/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
#[macro_use]
extern crate lazy_static;

mod util;
use std::sync::Once;

use crate::util::vegajs_runtime::{vegajs_runtime, ExportImageFormat};
use datafusion::scalar::ScalarValue;
use rstest::rstest;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::sync::Arc;
use tokio::runtime::Runtime;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::data::table::VegaFusionTable;

use vegafusion_core::planning::plan::SpecPlan;

use vegafusion_core::planning::watch::{
    ExportUpdate, ExportUpdateBatch, ExportUpdateNamespace, Watch, WatchNamespace, WatchPlan,
};
use vegafusion_core::proto::gen::pretransform::{PreTransformSpecOpts, PreTransformSpecRequest};
use vegafusion_core::proto::gen::services::pre_transform_spec_result;
use vegafusion_core::proto::gen::tasks::{TaskGraph, TzConfig};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

static INIT: Once = Once::new();

pub fn initialize() {
    INIT.call_once(|| {
        // Delete and remake empty output image directory
        let output_dir = format!("{}/tests/output/", crate_dir());
        std::fs::remove_dir_all(&output_dir).ok();
        std::fs::create_dir(&output_dir).expect("Failed to create output directory");
    });
}

#[cfg(test)]
mod test_custom_specs {
    use super::*;

    #[rstest(
        spec_name,
        tolerance,
        case("custom/stacked_bar", 0.001),
        case("custom/bar_colors", 0.001),
        case("custom/imdb_histogram", 0.001),
        case("custom/flights_crossfilter_a", 0.001),
        case("custom/flights_crossfilter_csv", 0.001),
        case("custom/log_scaled_histogram", 0.001),
        case("custom/non_linear_histogram", 0.001),
        case("custom/relative_frequency_histogram", 0.001),
        case("custom/kde_iris", 0.001),
        case("custom/2d_circles_histogram_imdb", 0.001),
        case("custom/2d_histogram_imdb", 0.001),
        case("custom/cumulative_window_imdb", 0.001),
        case("custom/density_and_cumulative_histograms", 0.001),
        case("custom/mean_strip_plot_movies", 0.001),
        case("custom/table_heatmap_cars", 0.001),
        case("custom/difference_from_mean", 0.001),
        case("custom/nested_concat_align", 0.001),
        case("custom/imdb_dashboard_cross_height", 0.001),
        case("custom/stacked_bar_weather_year", 0.001),
        case("custom/stacked_bar_weather_month", 0.001),
        case("custom/stacked_bar_normalize", 0.001),
        case("custom/layer_bar_labels_grey", 0.001),
        case("custom/bar_month_temporal_initial", 0.001),
        case("custom/selection_layer_bar_month", 0.001),
        case("custom/interactive_layered_crossfilter", 0.001),
        case("custom/interactive_seattle_weather", 0.001),
        case("custom/concat_marginal_histograms", 0.001),
        case("custom/joinaggregate_movie_rating", 0.001),
        case("custom/joinaggregate_text_color_contrast", 0.001),
        case("custom/cumulative_running_window", 0.001),
        case("custom/point_bubble", 0.001),
        case("custom/circle_bubble_health_income", 0.001),
        case("custom/line_color_stocks", 0.001),
        case("custom/line_slope_barley", 0.001),
        case("custom/connected_scatterplot", 0.001),
        case("custom/layer_line_co2_concentration", 0.001),
        case("custom/window_rank_matches", 0.001),
        case("custom/circle_github_punchcard", 0.001),
        case("custom/rect_lasagna", 0.001),
        case("custom/rect_heatmap_weather", 0.001),
        case("custom/layer_line_rolling_mean_point_raw", 0.001),
        case("custom/layer_histogram_global_mean", 0.001),
        case("custom/layer_precipitation_mean", 0.001),
        case("custom/wheat_wages", 0.001),
        case("custom/trellis_stacked_bar", 0.001),
        case("custom/trellis_bar_histogram", 0.001),
        case("custom/interactive_average", 0.001),
        case("custom/histogram_responsive", 0.001),
        case("custom/grouped_bar_chart_with_error_bars", 0.001),
        case("custom/one_dot_per_zipcode", 0.001),
        case("custom/ridgeline", 0.001),
        case("custom/binned_scatter", 0.001),
        case("custom/seattle_temps_heatmap", 0.001),
        case("custom/movies_agg_parameterize", 0.001),
        case("custom/escaped_column_name1", 0.001),
        case("custom/layered_movies", 0.001),
        case("custom/shipping_mixed_scales", 0.001),
        case("custom/datum_color", 0.001),
        case("custom/bug_153", 0.001)
    )]
    fn test_image_comparison(spec_name: &str, tolerance: f64) {
        println!("spec_name: {}", spec_name);
        TOKIO_RUNTIME.block_on(check_spec_sequence_from_files(spec_name, tolerance));
        TOKIO_RUNTIME.block_on(check_pre_transform_spec_from_files(spec_name, tolerance));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_vega_specs {
    use super::*;

    #[rstest(
        spec_name, tolerance,
        case("vega/airports", 0.001),

        // // window transform fields isn't optional
        // case("vega/arc-diagram", 0.001),
        // case("vega/matrix-reorder", 0.001),

        case("vega/arc", 0.001),
        case("vega/area", 0.001),
        case("vega/autosize-fit", 0.001),
        case("vega/autosize-fit-x", 0.001),
        case("vega/autosize-fit-y", 0.001),
        case("vega/bar-hover-label", 0.001),
        case("vega/barley", 0.001),
        case("vega/bar-rangestep", 0.001),

        // // timeunit transform's unit field can't be a signal
        // case("vega/bar-time", 0.001),

        case("vega/bar", 0.001),

        // // aggregate transform's groupby field isn't optional
        // case("vega/box-plot", 0.001),
        // case("vega/budget-forecasts", 0.001),
        // case("vega/calendar", 0.001),
        // case("vega/density", 0.001),
        // case("vega/hops", 0.001),
        // case("vega/movies-sort", 0.001),
        // case("vega/nulls-scatter-plot", 0.001),

        // // Schema for signal.on.update not flexible enough
        // case("vega/chart-rangestep", 0.001),
        case("vega/chart", 0.001),

        case("vega/choropleth", 0.001),
        case("vega/contour-map", 0.001),
        case("vega/contour-scatter", 0.001),
        case("vega/corner-radius", 0.001),

        // // bin transform's step property cannot be a signal
        // case("vega/crossfilter-multi", 0.001),
        // case("vega/crossfilter", 0.001),
        // case("vega/dot-plot", 0.001),
        // case("vega/histogram", 0.001),

        // // Something messed up in dependencies of complex signal chain
        // case("vega/dimpvis", 0.001),

        case("vega/driving", 0.001),
        case("vega/dynamic-format", 0.001),

        // // Dynamic URL as signal not supported
        // case("vega/dynamic-url", 0.001),
        // case("vega/quantile-quantile-plot", 0.001),

        case("vega/error", 0.001),
        case("vega/falkensee", 0.001),
        case("vega/flush-axis-labels", 0.001),
        case("vega/font-size-steps", 0.001),

        // // The "property" option for loading JSON array from an object property is not supported
        // case("vega/force-beeswarm", 0.001),
        // case("vega/force-network", 0.001),
        // case("vega/isocontour-precipitation", 0.001),
        // case("vega/isocontour-volcano", 0.001),

        // // The tilda prefix operator (Bitwise NOT) is not supported
        // case("vega/gapminder", 0.001),
        // case("vega/wordcloud", 0.001),

        case("vega/gradient", 0.001),
        case("vega/grouped-bar", 0.001),
        case("vega/heatmap-image", 0.001),

        // // Looks like there might be a timezone issue
        // case("vega/heatmap-lines", 0.001),

        case("vega/heatmap-sinusoids", 0.001),
        case("vega/heatmap", 0.001),
        case("vega/horizon", 0.001),

        // // Error from vega-scenegraph: Image given has not completed loading
        // case("vega/images-inline", 0.001),

        case("vega/images", 0.001),
        case("vega/isocontour-airports", 0.001),
        case("vega/jobs", 0.001),
        case("vega/kde", 0.001),
        case("vega/label", 0.001),
        case("vega/layout-facet", 0.001),
        case("vega/layout-hconcat", 0.001),

        // // The 'innerX' scale is defined in a group but used in a top-level signal.
        // // Not sure why this works in Vega
        // case("vega/layout-splom", 0.001),
        // case("vega/splom-inner", 0.001),

        case("vega/layout-vconcat", 0.001),
        case("vega/layout-wrap", 0.001),
        case("vega/legends-continuous", 0.001),
        case("vega/legends-discrete", 0.001),
        case("vega/legends-ordinal", 0.001),
        case("vega/legends-symbol", 0.001),
        case("vega/legends", 0.001),
        case("vega/lifelines", 0.001),
        case("vega/map-area-compare", 0.001),
        case("vega/map-bind", 0.001),
        case("vega/map-fit", 0.001),

        // // Geo projections hang the nodejs runtime sometimes
        // case("vega/map-point-radius", 0.001),
        // case("vega/map", 0.001),

        case("vega/nested-plot", 0.001),

        // // bin transform's maxbins cannot be a signal
        // case("vega/nulls-histogram", 0.001),
        // case("vega/scales-bin", 0.001),

        // // Weird initialization order that that results in the chart initially displaying
        // // without a selection, then showing the selection.  This is visible in the plain
        // // vega case, but the saved image doesn't show it. In the VegaFusion case, the saved
        // // image shows in earlier state.  The VegaFusion case does update when used live.
        // case("vega/overview-detail-bins", 0.001),

        case("vega/overview-detail", 0.001),

        // // Parsing on encode events not supported
        // case("vega/panzoom", 0.001),

        // // Paring mark encoding with scale of datum not supported
        // case("vega/parallel-coords", 0.001),
        // case("vega/splom-outer", 0.001),

        // // Named mark used as a dataset not supported
        // case("vega/playfair", 0.001),

        case("vega/population", 0.001),

        //  // "field" property of stack transform is required
        // case("vega/quantile-dot-plot", 0.001),

        case("vega/regression", 0.001),
        case("vega/scales-discretize", 0.001),
        case("vega/scatter-brush-filter", 0.001),
        case("vega/scatter-brush-panzoom", 0.001),
        case("vega/scatter-plot-contours", 0.001),
        case("vega/scatter-plot-guides", 0.001),
        case("vega/scatter-plot-heatmap", 0.001),
        case("vega/scatter-plot", 0.001),
        case("vega/shift-select", 0.001),
        case("vega/stacked-area", 0.001),
        case("vega/stacked-bar", 0.001),
        case("vega/stocks-index", 0.001),
        case("vega/symbol-angle", 0.001),
        case("vega/titles", 0.001),
        case("vega/tree-cluster", 0.001),
        case("vega/treemap", 0.001),
        case("vega/tree-nest", 0.001),

        // // // parent signal variable not supported
        // case("vega/text-multiline", 0.001),
        // case("vega/tree-radial-bundle", 0.001),
        // case("vega/violin-plot", 0.001),

        // // Signal must have initial value
        // case("vega/tree-radial", 0.001),

        // // Struct columns not well supported
        // case("vega/weather", 0.001),

        // // Window transform op as a signal not supported
        // case("vega/window", 0.001),
    )]
    fn test_image_comparison(spec_name: &str, tolerance: f64) {
        println!("spec_name: {}", spec_name);
        TOKIO_RUNTIME.block_on(check_spec_sequence_from_files(spec_name, tolerance));
        TOKIO_RUNTIME.block_on(check_pre_transform_spec_from_files(spec_name, tolerance));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_vegalite_specs {
    use super::*;

    #[rstest(
        spec_name, tolerance,
        case("vegalite/airport_connections", 0.001),
        case("vegalite/arc_donut", 0.001),
        case("vegalite/arc_facet", 0.001),
        case("vegalite/arc_ordinal_theta", 0.001),
        case("vegalite/arc_params", 0.001),
        case("vegalite/arc_pie", 0.001),
        case("vegalite/arc_pie_pyramid", 0.001),
        case("vegalite/arc_radial", 0.001),
        case("vegalite/arc_radial_histogram", 0.001),
        case("vegalite/area", 0.001),
        case("vegalite/area_cumulative_freq", 0.001),
        case("vegalite/area_density", 0.001),
        case("vegalite/area_density_facet", 0.001),
        case("vegalite/area_density_stacked", 0.001),
        case("vegalite/area_density_stacked_fold", 0.001),
        case("vegalite/area_gradient", 0.001),
        case("vegalite/area_horizon", 0.001),
        case("vegalite/area_overlay", 0.001),
        case("vegalite/area_temperature_range", 0.001),
        case("vegalite/area_vertical", 0.001),
        case("vegalite/area", 0.001),
        case("vegalite/argmin_spaces", 0.001),
        case("vegalite/bar_1d_binned", 0.001),
        case("vegalite/bar_1d_step_config", 0.001),
        case("vegalite/bar_1d", 0.001),
        case("vegalite/bar_aggregate_count", 0.001),
        case("vegalite/bar_aggregate_format", 0.001),
        case("vegalite/bar_aggregate_size", 0.001),
        case("vegalite/bar_aggregate_sort_by_encoding", 0.001),
        case("vegalite/bar_aggregate_sort_mean", 0.001),
        case("vegalite/bar_aggregate_transform", 0.001),
        case("vegalite/bar_aggregate_vertical", 0.001),
        case("vegalite/bar_aggregate", 0.001),
        case("vegalite/bar_argmax_transform", 0.001),
        case("vegalite/bar_argmax", 0.001),
        case("vegalite/bar_array_aggregate", 0.001),
        case("vegalite/bar_axis_orient", 0.001),
        case("vegalite/bar_axis_space_saving", 0.001),
        case("vegalite/bar_binned_data", 0.001),
        case("vegalite/bar_bullet_expr_bind", 0.001),
        case("vegalite/bar_color_disabled_scale", 0.001),
        case("vegalite/bar_column_fold", 0.001),
        case("vegalite/bar_column_pivot", 0.001),
        case("vegalite/bar_corner_radius_end", 0.001),

        // Different ordering of bars with the same length
        case("vegalite/bar_count_minimap", 0.1),

        case("vegalite/bar_custom_sort_full", 0.001),
        case("vegalite/bar_custom_sort_partial", 0.001),
        case("vegalite/bar_custom_time_domain", 0.001),
        case("vegalite/bar_default_tooltip_title_null", 0.001),
        case("vegalite/bar_distinct", 0.001),
        case("vegalite/bar_diverging_stack_population_pyramid", 0.001),
        case("vegalite/bar_diverging_stack_transform", 0.001),
        case("vegalite/bar_filter_calc", 0.001),
        case("vegalite/bar_fit", 0.001),
        case("vegalite/bar_gantt", 0.001),
        case("vegalite/bar_grouped_custom_padding", 0.001),
        case("vegalite/bar_grouped_errorbar", 0.001),
        case("vegalite/bar_grouped_facet_independent_scale_fixed_width", 0.001),
        case("vegalite/bar_grouped_facet_independent_scale", 0.001),
        case("vegalite/bar_grouped_facet", 0.001),
        case("vegalite/bar_grouped_fixed_width", 0.001),
        case("vegalite/bar_grouped_horizontal", 0.001),
        case("vegalite/bar_grouped_label", 0.001),
        case("vegalite/bar_grouped_repeated", 0.001),
        case("vegalite/bar_grouped_stacked", 0.001),
        case("vegalite/bar_grouped_step_for_offset", 0.001),
        case("vegalite/bar_grouped_step_for_position", 0.001),
        case("vegalite/bar_grouped", 0.001),

        // (Different facet layer order)
        case("vegalite/bar_layered_transparent", 0.1),
        case("vegalite/bar_layered_weather", 0.001),
        case("vegalite/bar_month_band_config", 0.001),
        case("vegalite/bar_month_band", 0.001),
        case("vegalite/bar_month_temporal_initial", 0.001),
        case("vegalite/bar_month_temporal", 0.001),
        case("vegalite/bar_month", 0.001),
        case("vegalite/bar_multi_values_per_categories", 0.001),
        case("vegalite/bar_negative_horizontal_label", 0.001),
        case("vegalite/bar_negative", 0.001),
        case("vegalite/bar_params_bound", 0.001),
        case("vegalite/bar_params", 0.001),
        case("vegalite/bar_size_default", 0.001),
        case("vegalite/bar_size_explicit_bad", 0.001),
        case("vegalite/bar_size_fit", 0.001),
        case("vegalite/bar_size_responsive", 0.001),
        case("vegalite/bar_size_step_small", 0.001),
        case("vegalite/bar_sort_by_count", 0.001),
        case("vegalite/bar_swap_axes", 0.001),
        case("vegalite/bar_swap_custom", 0.001),
        case("vegalite/bar_title_start", 0.001),
        case("vegalite/bar_title", 0.001),
        case("vegalite/bar_tooltip_aggregate", 0.001),
        case("vegalite/bar_tooltip_groupby", 0.001),
        case("vegalite/bar_tooltip_multi", 0.001),
        case("vegalite/bar_tooltip_title", 0.001),
        case("vegalite/bar_tooltip", 0.001),
        case("vegalite/bar", 0.001),
        case("vegalite/bar_x_offset_without_x_broken", 0.001),
        case("vegalite/bar_yearmonth_custom_format", 0.001),
        case("vegalite/bar_yearmonth", 0.001),
        case("vegalite/boxplot_1D_horizontal_custom_mark", 0.001),
        case("vegalite/boxplot_1D_horizontal_explicit", 0.001),
        case("vegalite/boxplot_1D_horizontal", 0.001),
        case("vegalite/boxplot_1D_vertical", 0.001),
        case("vegalite/boxplot_2D_horizontal_color_size", 0.001),
        case("vegalite/boxplot_2D_horizontal", 0.001),
        case("vegalite/boxplot_2D_vertical", 0.001),
        case("vegalite/boxplot_groupped", 0.001),
        case("vegalite/boxplot_minmax_2D_horizontal_custom_midtick_color", 0.001),
        case("vegalite/boxplot_minmax_2D_horizontal", 0.001),
        case("vegalite/boxplot_minmax_2D_vertical", 0.001),
        case("vegalite/boxplot_preaggregated", 0.001),
        case("vegalite/boxplot_tooltip_aggregate", 0.001),
        case("vegalite/boxplot_tooltip_not_aggregate", 0.001),
        case("vegalite/brush_table", 0.001),
        case("vegalite/circle_binned_maxbins_20", 0.001),
        case("vegalite/circle_binned_maxbins_2", 0.001),
        case("vegalite/circle_binned_maxbins_5", 0.001),
        case("vegalite/circle_binned", 0.001),
        case("vegalite/circle_bubble_health_income", 0.001),
        case("vegalite/circle_custom_tick_labels", 0.001),
        case("vegalite/circle_flatten", 0.001),
        case("vegalite/circle_github_punchcard", 0.001),
        case("vegalite/circle_labelangle_orient_signal", 0.001),
        case("vegalite/circle_natural_disasters", 0.001),
        case("vegalite/circle_opacity", 0.001),
        case("vegalite/circle_scale_quantile", 0.001),
        case("vegalite/circle_scale_quantize", 0.001),
        case("vegalite/circle_scale_threshold", 0.001),
        case("vegalite/circle", 0.001),
        case("vegalite/circle_wilkinson_dotplot_stacked", 0.001),
        case("vegalite/circle_wilkinson_dotplot", 0.001),
        case("vegalite/concat_bar_layer_circle", 0.001),
        case("vegalite/concat_bar_scales_discretize_2_cols", 0.001),
        case("vegalite/concat_bar_scales_discretize", 0.001),
        case("vegalite/concat_hover_filter", 0.001),
        case("vegalite/concat_hover", 0.001),
        case("vegalite/concat_layer_voyager_result", 0.001),
        case("vegalite/concat_marginal_histograms", 0.001),
        case("vegalite/concat_population_pyramid", 0.001),
        case("vegalite/concat_weather", 0.001),
        case("vegalite/connected_scatterplot", 0.001),
        case("vegalite/embedded_csv", 0.001),

        // // (ci function is non-deterministic and cause non-deterministic image size)
        // case("vegalite/errorband_2d_horizontal_color_encoding", 0.5),
        // case("vegalite/errorband_2d_vertical_borders", 0.5),
        // case("vegalite/errorband_tooltip", 0.5),
        // case("vegalite/errorbar_2d_vertical_ticks", 0.5),

        case("vegalite/errorbar_aggregate", 0.001),
        case("vegalite/errorbar_horizontal_aggregate", 0.001),
        case("vegalite/errorbar_tooltip", 0.001),
        case("vegalite/facet_bullet", 0.001),
        case("vegalite/facet_column_facet_column_point_future", 0.001),
        case("vegalite/facet_column_facet_row_point_future", 0.001),
        case("vegalite/facet_cross_independent_scale", 0.001),
        case("vegalite/facet_custom_header", 0.001),
        case("vegalite/facet_custom", 0.001),
        case("vegalite/facet_grid_bar", 0.001),
        case("vegalite/facet_independent_scale_layer_broken", 0.001),

        case("vegalite/facet_independent_scale", 0.001),
        case("vegalite/facet_row_facet_row_point_future", 0.001),
        case("vegalite/geo_choropleth", 0.001),
        case("vegalite/geo_circle", 0.001),
        case("vegalite/geo_constant_value", 0.001),
        case("vegalite/geo_custom_projection", 0.001),
        case("vegalite/geo_graticule_object", 0.001),
        case("vegalite/geo_graticule", 0.001),
        case("vegalite/geo_layer_line_london", 0.001),
        case("vegalite/geo_layer", 0.001),
        case("vegalite/geo_line", 0.001),
        case("vegalite/geo_params_projections", 0.001),
        case("vegalite/geo_point", 0.001),
        case("vegalite/geo_repeat", 0.001),
        case("vegalite/geo_rule", 0.001),
        case("vegalite/geo_sphere", 0.001),
        case("vegalite/geo_text", 0.001),
        case("vegalite/geo_trellis", 0.001),
        case("vegalite/hconcat_weather", 0.001),
        case("vegalite/histogram_bin_change", 0.001),
        case("vegalite/histogram_bin_spacing_reverse", 0.001),
        case("vegalite/histogram_bin_spacing", 0.001),
        case("vegalite/histogram_bin_transform", 0.001),
        case("vegalite/histogram_invalid_null", 0.001),
        case("vegalite/histogram_log", 0.001),
        case("vegalite/histogram_nonlinear", 0.001),
        case("vegalite/histogram_no_spacing", 0.001),
        case("vegalite/histogram_ordinal_sort", 0.001),
        case("vegalite/histogram_ordinal", 0.001),
        case("vegalite/histogram_rel_freq", 0.001),
        case("vegalite/histogram_reverse", 0.001),
        case("vegalite/histogram", 0.001),
        case("vegalite/interactive_area_brush", 0.001),
        case("vegalite/interactive_bar_select_highlight", 0.001),
        case("vegalite/interactive_bin_extent_bottom", 0.001),
        case("vegalite/interactive_bin_extent", 0.001),
        case("vegalite/interactive_brush", 0.001),
        case("vegalite/interactive_concat_layer", 0.001),
        case("vegalite/interactive_dashboard_europe_pop", 0.001),
        case("vegalite/interactive_global_development", 0.001),
        case("vegalite/interactive_index_chart", 0.001),
        case("vegalite/interactive_layered_crossfilter_discrete", 0.001),
        case("vegalite/interactive_layered_crossfilter", 0.001),
        case("vegalite/interactive_legend_dblclick", 0.001),
        case("vegalite/interactive_legend", 0.001),
        case("vegalite/interactive_line_brush_cursor", 0.001),
        case("vegalite/interactive_line_hover", 0.001),

        // (subpixel shift if y-axis grid line)
        case("vegalite/interactive_multi_line_label", 0.003),

        case("vegalite/interactive_multi_line_pivot_tooltip", 0.001),
        case("vegalite/interactive_multi_line_tooltip", 0.001),
        case("vegalite/interactive_overview_detail", 0.001),
        case("vegalite/interactive_paintbrush_color_nearest", 0.001),
        case("vegalite/interactive_paintbrush_color", 0.001),
        case("vegalite/interactive_paintbrush_interval", 0.001),
        case("vegalite/interactive_paintbrush_simple_false", 0.001),
        case("vegalite/interactive_paintbrush_simple_true", 0.001),
        case("vegalite/interactive_paintbrush", 0.001),
        case("vegalite/interactive_panzoom_splom", 0.001),
        case("vegalite/interactive_panzoom_vconcat_shared", 0.001),
        case("vegalite/interactive_point_init", 0.001),
        case("vegalite/interactive_query_widgets", 0.001),
        case("vegalite/interactive_seattle_weather", 0.001),
        case("vegalite/interactive_splom", 0.001),
        case("vegalite/interactive_stocks_nearest_index", 0.001),
        case("vegalite/isotype_bar_chart_emoji", 0.001),
        case("vegalite/isotype_bar_chart", 0.001),
        case("vegalite/isotype_grid", 0.001),
        case("vegalite/joinaggregate_mean_difference_by_year", 0.001),
        case("vegalite/joinaggregate_mean_difference", 0.001),
        case("vegalite/joinaggregate_percent_of_total", 0.001),
        case("vegalite/joinaggregate_residual_graph", 0.001),
        case("vegalite/layer_arc_label", 0.001),
        case("vegalite/layer_bar_annotations", 0.001),
        case("vegalite/layer_bar_circle_grouped", 0.001),
        case("vegalite/layer_bar_fruit", 0.001),
        case("vegalite/layer_bar_grouped_line_ungrouped", 0.001),
        case("vegalite/layer_bar_labels_grey", 0.001),
        case("vegalite/layer_bar_labels_style", 0.001),
        case("vegalite/layer_bar_labels", 0.001),
        case("vegalite/layer_bar_line_union", 0.001),
        case("vegalite/layer_bar_line", 0.001),
        case("vegalite/layer_bar_month", 0.001),
        case("vegalite/layer_bar_tick_datum_grouped", 0.001),
        case("vegalite/layer_boxplot_circle", 0.001),
        case("vegalite/layer_candlestick", 0.001),
        case("vegalite/layer_circle_independent_color", 0.001),
        case("vegalite/layer_color_legend_left", 0.001),
        case("vegalite/layer_cumulative_histogram", 0.001),
        case("vegalite/layer_dual_axis", 0.001),
        case("vegalite/layer_falkensee", 0.001),
        case("vegalite/layer_histogram_global_mean", 0.001),
        case("vegalite/layer_histogram", 0.001),
        case("vegalite/layer_likert", 0.001),
        case("vegalite/layer_line_co2_concentration", 0.001),
        case("vegalite/layer_line_color_rule", 0.001),
        case("vegalite/layer_line_datum_rule_datetime", 0.001),
        case("vegalite/layer_line_datum_rule", 0.001),

        // // (Offset in start position of outline dash)
        // case("vegalite/layer_line_errorband_2d_horizontal_borders_strokedash", 0.5),

        case("vegalite/layer_line_errorband_pre_aggregated", 0.001),
        case("vegalite/layer_line_mean_point_raw", 0.001),
        case("vegalite/layer_line_rolling_mean_point_raw", 0.001),
        case("vegalite/layer_line_window", 0.001),
        case("vegalite/layer_overlay", 0.001),
        case("vegalite/layer_point_errorbar_1d_horizontal", 0.001),
        case("vegalite/layer_point_errorbar_1d_vertical", 0.001),

        // // (ci non-deterministic)
        // case("vegalite/layer_line_errorband_ci", 0.5),
        // case("vegalite/layer_point_errorbar_2d_horizontal_ci", 0.5),
        // case("vegalite/layer_point_errorbar_ci", 0.5),

        case("vegalite/layer_point_errorbar_2d_horizontal_color_encoding", 0.001),
        case("vegalite/layer_point_errorbar_2d_horizontal_custom_ticks", 0.001),
        case("vegalite/layer_point_errorbar_2d_horizontal_iqr", 0.001),
        case("vegalite/layer_point_errorbar_2d_horizontal_stdev", 0.001),
        case("vegalite/layer_scatter_errorband_1D_stdev_global_mean", 0.001),
        case("vegalite/layer_point_errorbar_stdev", 0.001),
        case("vegalite/layer_point_errorbar_2d_horizontal", 0.001),
        case("vegalite/layer_point_errorbar_2d_vertical", 0.001),
        case("vegalite/layer_point_errorbar_pre_aggregated_asymmetric_error", 0.001),
        case("vegalite/layer_point_errorbar_pre_aggregated_symmetric_error", 0.001),
        case("vegalite/layer_point_errorbar_pre_aggregated_upper_lower", 0.001),
        case("vegalite/layer_point_line_loess", 0.001),
        case("vegalite/layer_point_line_regression", 0.001),
        case("vegalite/layer_precipitation_mean", 0.001),
        case("vegalite/layer_ranged_dot", 0.001),
        case("vegalite/layer_rect_extent", 0.001),
        case("vegalite/layer_scatter_errorband_1d_stdev", 0.001),
        case("vegalite/layer_single_color", 0.001),
        case("vegalite/layer_text_heatmap_joinaggregate", 0.001),
        case("vegalite/layer_text_heatmap", 0.001),
        case("vegalite/layer_timeunit_rect", 0.001),
        case("vegalite/line_bump", 0.001),
        case("vegalite/line_calculate", 0.001),
        case("vegalite/line_color_binned", 0.001),
        case("vegalite/line_color_halo", 0.001),
        case("vegalite/line_color_label", 0.001),
        case("vegalite/line_color", 0.001),
        case("vegalite/line_concat_facet", 0.001),
        case("vegalite/line_conditional_axis_config", 0.001),
        case("vegalite/line_conditional_axis", 0.001),
        case("vegalite/line_dashed_part", 0.001),
        case("vegalite/line_detail", 0.001),
        case("vegalite/line_domainminmax", 0.001),
        case("vegalite/line_encoding_impute_keyvals_sequence", 0.001),
        case("vegalite/line_encoding_impute_keyvals", 0.001),
        case("vegalite/line_impute_frame", 0.001),
        case("vegalite/line_impute_keyvals", 0.001),
        case("vegalite/line_impute_method", 0.001),
        case("vegalite/line_impute_transform_frame", 0.001),
        case("vegalite/line_impute_transform_value", 0.001),
        case("vegalite/line_impute_value", 0.001),
        case("vegalite/line_inside_domain_using_clip", 0.001),
        case("vegalite/line_inside_domain_using_transform", 0.001),
        case("vegalite/line_max_year", 0.001),
        case("vegalite/line_mean_month", 0.001),
        case("vegalite/line_mean_year", 0.001),
        case("vegalite/line_monotone", 0.001),
        case("vegalite/line_month_center_band", 0.001),
        case("vegalite/line_month", 0.001),
        case("vegalite/line_outside_domain", 0.001),

        // (Offset in start position of outline dash)
        case("vegalite/line_overlay_stroked", 0.01),

        case("vegalite/line_overlay", 0.001),
        case("vegalite/line_params", 0.001),
        case("vegalite/line_quarter_legend", 0.001),
        case("vegalite/line_shape_overlay", 0.001),
        case("vegalite/line_skip_invalid_mid_cap_square", 0.001),
        case("vegalite/line_skip_invalid_mid_overlay", 0.001),
        case("vegalite/line_skip_invalid_mid", 0.001),
        case("vegalite/line_skip_invalid", 0.001),
        case("vegalite/line_slope", 0.001),
        case("vegalite/line_sort_axis", 0.001),
        case("vegalite/line_step", 0.001),
        case("vegalite/line_strokedash", 0.001),
        case("vegalite/line_timestamp_domain", 0.001),
        case("vegalite/line_timeunit_transform", 0.001),
        case("vegalite/line", 0.001),
        case("vegalite/lookup", 0.001),
        case("vegalite/nested_concat_align", 0.001),
        case("vegalite/parallel_coordinate", 0.001),
        case("vegalite/param_expr", 0.001),
        case("vegalite/point_1d_array", 0.001),
        case("vegalite/point_1d", 0.001),
        case("vegalite/point_2d_aggregate", 0.001),
        case("vegalite/point_2d_array_named", 0.001),
        case("vegalite/point_2d_array", 0.001),
        case("vegalite/point_2d_tooltip_data", 0.001),
        case("vegalite/point_2d_tooltip", 0.001),
        case("vegalite/point_2d", 0.001),
        case("vegalite/point_aggregate_detail", 0.001),
        case("vegalite/point_angle_windvector", 0.001),
        case("vegalite/point_background", 0.001),
        case("vegalite/point_binned_color", 0.001),
        case("vegalite/point_binned_opacity", 0.001),
        case("vegalite/point_binned_size", 0.001),
        case("vegalite/point_bubble", 0.001),
        case("vegalite/point_color_custom", 0.001),
        case("vegalite/point_color_ordinal", 0.001),
        case("vegalite/point_color_quantitative", 0.001),
        case("vegalite/point_colorramp_size", 0.001),
        case("vegalite/point_color_shape_constant", 0.001),
        case("vegalite/point_color", 0.001),
        case("vegalite/point_color_with_shape", 0.001),
        case("vegalite/point_diverging_color", 0.001),
        case("vegalite/point_dot_timeunit_color", 0.001),
        case("vegalite/point_filled", 0.001),
        case("vegalite/point_grouped", 0.001),
        case("vegalite/point_href", 0.001),
        case("vegalite/point_invalid_color", 0.001),
        case("vegalite/point_log", 0.001),
        case("vegalite/point_no_axis_domain_grid", 0.001),

        // (random function is non-deterministic so use higher tolerance)
        case("vegalite/point_offset_random", 0.3),
        case("vegalite/point_ordinal_bin_offset_random", 0.3),

        case("vegalite/point_ordinal_color", 0.001),
        case("vegalite/point_overlap", 0.001),
        case("vegalite/point_params", 0.001),
        case("vegalite/point_quantile_quantile", 0.001),
        case("vegalite/point_scale_range_field", 0.001),
        case("vegalite/point_shape_custom", 0.001),
        case("vegalite/point_tooltip", 0.001),
        case("vegalite/rect_binned_heatmap", 0.001),
        case("vegalite/rect_heatmap", 0.001),
        case("vegalite/rect_heatmap_weather", 0.001),
        case("vegalite/rect_lasagna", 0.001),
        case("vegalite/rect_mosaic_labelled", 0.001),
        case("vegalite/rect_mosaic_labelled_with_offset", 0.001),
        case("vegalite/rect_mosaic_simple", 0.001),
        case("vegalite/rect_params", 0.001),
        case("vegalite/repeat_child_layer", 0.001),
        case("vegalite/repeat_histogram_autosize", 0.001),
        case("vegalite/repeat_histogram_flights", 0.001),
        case("vegalite/repeat_histogram", 0.001),
        case("vegalite/repeat_independent_colors", 0.001),
        case("vegalite/repeat_layer", 0.001),
        case("vegalite/repeat_line_weather", 0.001),
        case("vegalite/repeat_splom_cars", 0.001),
        case("vegalite/repeat_splom", 0.001),
        case("vegalite/rule_color_mean", 0.001),
        case("vegalite/rule_extent", 0.001),
        case("vegalite/rule_params", 0.001),

        // // (sample transform is non-deterministic, and causes non-deterministic image size)
        // case("vegalite/sample_scatterplot", 0.5),

        case("vegalite/scatter_image", 0.001),
        case("vegalite/selection_bind_cylyr", 0.001),
        case("vegalite/selection_bind_origin", 0.001),
        case("vegalite/selection_brush_timeunit", 0.001),
        case("vegalite/selection_clear_brush", 0.001),
        case("vegalite/selection_composition_and", 0.001),
        case("vegalite/selection_composition_or", 0.001),
        case("vegalite/selection_concat", 0.001),
        case("vegalite/selection_filter_composition", 0.001),
        case("vegalite/selection_filter_false", 0.001),
        case("vegalite/selection_filter_true", 0.001),
        case("vegalite/selection_filter", 0.001),
        case("vegalite/selection_heatmap", 0.001),
        case("vegalite/selection_insert", 0.001),
        case("vegalite/selection_interval_mark_style", 0.001),
        case("vegalite/selection_layer_bar_month", 0.001),
        case("vegalite/selection_multi_condition", 0.001),
        case("vegalite/selection_project_binned_interval", 0.001),
        case("vegalite/selection_project_interval", 0.001),
        case("vegalite/selection_project_interval_x", 0.001),
        case("vegalite/selection_project_interval_x_y", 0.001),
        case("vegalite/selection_project_interval_y", 0.001),
        case("vegalite/selection_project_multi_cylinders_origin", 0.001),
        case("vegalite/selection_project_multi_cylinders", 0.001),
        case("vegalite/selection_project_multi_origin", 0.001),
        case("vegalite/selection_project_multi", 0.001),
        case("vegalite/selection_project_single_cylinders_origin", 0.001),
        case("vegalite/selection_project_single_cylinders", 0.001),
        case("vegalite/selection_project_single_origin", 0.001),
        case("vegalite/selection_project_single", 0.001),
        case("vegalite/selection_resolution_global", 0.001),
        case("vegalite/selection_resolution_intersect", 0.001),
        case("vegalite/selection_resolution_union", 0.001),
        case("vegalite/selection_toggle_altKey_shiftKey", 0.001),
        case("vegalite/selection_toggle_altKey", 0.001),
        case("vegalite/selection_toggle_shiftKey", 0.001),
        case("vegalite/selection_translate_brush_drag", 0.001),
        case("vegalite/selection_translate_brush_shift_drag", 0.001),
        case("vegalite/selection_translate_scatterplot_drag", 0.001),
        case("vegalite/selection_translate_scatterplot_shift_drag", 0.001),
        case("vegalite/selection_type_interval_invert", 0.001),
        case("vegalite/selection_type_interval", 0.001),
        case("vegalite/selection_type_point", 0.001),
        case("vegalite/selection_type_single_dblclick", 0.001),
        case("vegalite/selection_type_single_mouseover", 0.001),
        case("vegalite/selection_zoom_brush_shift_wheel", 0.001),
        case("vegalite/selection_zoom_brush_wheel", 0.001),
        case("vegalite/selection_zoom_scatterplot_shift_wheel", 0.001),
        case("vegalite/selection_zoom_scatterplot_wheel", 0.001),
        case("vegalite/sequence_line_fold", 0.001),
        case("vegalite/sequence_line", 0.001),
        case("vegalite/square", 0.001),
        case("vegalite/stacked_area_normalize", 0.001),
        case("vegalite/stacked_area_ordinal", 0.001),
        case("vegalite/stacked_area_overlay", 0.001),
        case("vegalite/stacked_area_stream", 0.001),
        case("vegalite/stacked_area", 0.001),
        case("vegalite/stacked_bar_1d", 0.001),
        case("vegalite/stacked_bar_count_corner_radius_config", 0.001),
        case("vegalite/stacked_bar_count_corner_radius_mark", 0.001),
        case("vegalite/stacked_bar_count_corner_radius_mark_x", 0.001),
        case("vegalite/stacked_bar_count_corner_radius_stroke", 0.001),
        case("vegalite/stacked_bar_count", 0.001),
        case("vegalite/stacked_bar_h_normalized_labeled", 0.001),

        // (Legend matches, but difference stack order)
        case("vegalite/stacked_bar_h_order_custom", 0.001),

        case("vegalite/stacked_bar_h_order", 0.001),
        case("vegalite/stacked_bar_h", 0.001),
        case("vegalite/stacked_bar_normalize", 0.001),
        case("vegalite/stacked_bar_population_transform", 0.001),
        case("vegalite/stacked_bar_population", 0.001),
        case("vegalite/stacked_bar_size", 0.001),
        case("vegalite/stacked_bar_sum_opacity", 0.001),
        case("vegalite/stacked_bar_unaggregate", 0.001),
        case("vegalite/stacked_bar_v", 0.001),
        case("vegalite/stacked_bar_weather", 0.001),
        case("vegalite/test_aggregate_nested", 0.001),
        case("vegalite/test_field_with_spaces", 0.001),
        case("vegalite/test_single_point_color", 0.001),
        case("vegalite/test_subobject_missing", 0.001),
        case("vegalite/test_subobject_nested", 0.001),
        case("vegalite/test_subobject", 0.001),
        case("vegalite/text_format", 0.001),
        case("vegalite/text_params", 0.001),
        case("vegalite/text_scatterplot_colored", 0.001),
        case("vegalite/tick_dot_thickness", 0.001),
        case("vegalite/tick_dot", 0.001),
        case("vegalite/tick_grouped", 0.001),
        case("vegalite/tick_sort", 0.001),
        case("vegalite/tick_strip_tick_band", 0.001),
        case("vegalite/tick_strip", 0.001),
        case("vegalite/time_custom_step", 0.001),
        case("vegalite/time_output_utc_scale", 0.001),
        case("vegalite/time_output_utc_timeunit", 0.001),
        case("vegalite/time_parse_local", 0.001),
        case("vegalite/time_parse_utc_format", 0.001),
        case("vegalite/time_parse_utc", 0.001),
        case("vegalite/trail_color", 0.001),
        case("vegalite/trail_comet", 0.001),
        case("vegalite/trellis_anscombe", 0.001),

        // (Use of undefined "order" field in aggregate transform is
        //  undefined in Vega and an error in VegaFusion)
        // // case("trellis_area_seattle", 0.001),

        case("vegalite/trellis_area_sort_array", 0.001),
        case("vegalite/trellis_area", 0.001),
        case("vegalite/trellis_bar_histogram_label_rotated", 0.001),
        case("vegalite/trellis_bar_histogram", 0.001),
        case("vegalite/trellis_barley_independent", 0.001),
        case("vegalite/trellis_barley_layer_median", 0.001),
        case("vegalite/trellis_barley", 0.001),
        case("vegalite/trellis_bar_no_header", 0.001),
        case("vegalite/trellis_bar", 0.001),
        case("vegalite/trellis_column_year", 0.001),
        case("vegalite/trellis_cross_sort_array", 0.001),
        case("vegalite/trellis_cross_sort", 0.001),
        case("vegalite/trellis_line_quarter", 0.001),
        case("vegalite/trellis_row_column", 0.001),
        case("vegalite/trellis_scatter_binned_row", 0.001),
        case("vegalite/trellis_scatter_small", 0.001),
        case("vegalite/trellis_scatter", 0.001),
        case("vegalite/trellis_selections", 0.001),
        case("vegalite/trellis_stacked_bar", 0.001),
        case("vegalite/vconcat_flatten", 0.001),
        case("vegalite/vconcat_weather", 0.001),
        case("vegalite/waterfall_chart", 0.001),
        case("vegalite/wheat_wages", 0.001),
        case("vegalite/window_cumulative_running_average", 0.001),
        case("vegalite/window_percent_of_total", 0.001),
        case("vegalite/window_rank", 0.001),
        case("vegalite/window_top_k_others", 0.001),
        case("vegalite/window_top_k", 0.001),
    )]
    fn test_image_comparison(spec_name: &str, tolerance: f64) {
        println!("spec_name: {}", spec_name);
        TOKIO_RUNTIME.block_on(check_spec_sequence_from_files(spec_name, tolerance));
        TOKIO_RUNTIME.block_on(check_pre_transform_spec_from_files(spec_name, tolerance));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[rustfmt::skip]  // Rust format breaks the rstest macro use below
#[cfg(test)]
mod test_image_comparison_timeunit {
    use super::*;
    use itertools::Itertools;
    use vegafusion_core::spec::transform::timeunit::{TimeUnitTimeZoneSpec, TimeUnitUnitSpec};
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest]
    fn test_image_comparison(
        #[values(
            vec![TimeUnitUnitSpec::Year],
            vec![TimeUnitUnitSpec::Quarter],
            vec![TimeUnitUnitSpec::Month],
            vec![TimeUnitUnitSpec::Week],
            vec![TimeUnitUnitSpec::Date],
            vec![TimeUnitUnitSpec::Day],
            vec![TimeUnitUnitSpec::Year, TimeUnitUnitSpec::Quarter],
            vec![TimeUnitUnitSpec::Year, TimeUnitUnitSpec::Month],
            vec![TimeUnitUnitSpec::Year, TimeUnitUnitSpec::Week],
        )]
        units: Vec<TimeUnitUnitSpec>,

        #[values(
            TimeUnitTimeZoneSpec::Utc,
            TimeUnitTimeZoneSpec::Local,
        )]
        timezone: TimeUnitTimeZoneSpec,

        #[values(
            "custom/bar_month_temporal_initial_parameterize",
            "custom/bar_month_temporal_initial_parameterize_local",
            "custom/stacked_bar_weather_timeunit_parameterize",
            "custom/stacked_bar_weather_timeunit_parameterize_local",
        )]
        spec_name: &str,
    ) {
        initialize();

        // Load spec
        let mut full_spec = load_spec(spec_name);

        // Load updates
        let full_updates = load_updates(spec_name);

        // Load expected watch plan
        let watch_plan = load_expected_watch_plan(spec_name);

        // Modify transform spec
        let num_data = full_spec.data.len();
        let timeunit_tx = full_spec
            .data
            .get_mut( num_data - 1)
            .unwrap()
            .transform
            .get_mut(0)
            .unwrap();
        if let TransformSpec::Timeunit(timeunit_tx) = timeunit_tx {
            timeunit_tx.units = Some(units.clone());
            timeunit_tx.timezone = Some(timezone.clone());
        } else {
            panic!("Unexpected transform")
        }

        // Build name for saved images
        let units_str = units
            .iter()
            .map(|unit| {
                let s = serde_json::to_string(unit).unwrap();
                s.trim_matches('"').to_string()
            })
            .join("_");
        let timezone_str = serde_json::to_string(&timezone)
            .unwrap()
            .trim_matches('"')
            .to_string();
        let output_name = format!("{}_timeunit_{}_{}", spec_name, units_str, timezone_str);

        TOKIO_RUNTIME.block_on(check_spec_sequence(
            full_spec,
            full_updates,
            watch_plan,
            &output_name,
            0.001,
        ));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[rustfmt::skip]  // Rust format breaks the rstest macro use below
#[cfg(test)]
mod test_image_comparison_agg {
    use super::*;
    use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest]
    fn test_image_comparison(
        #[values(
            AggregateOpSpec::Count,
            AggregateOpSpec::Valid,
            AggregateOpSpec::Missing,
            AggregateOpSpec::Distinct,
            AggregateOpSpec::Sum,
            AggregateOpSpec::Mean,
            AggregateOpSpec::Average,
            AggregateOpSpec::Variance,
            AggregateOpSpec::Variancep,
            AggregateOpSpec::Stdev,
            AggregateOpSpec::Stdevp,
        )]
        agg: AggregateOpSpec,

        #[values(
            "custom/movies_agg_parameterize",
        )]
        spec_name: &str,
    ) {
        initialize();

        // Load spec
        let mut full_spec = load_spec(spec_name);

        // Load updates
        let full_updates = load_updates(spec_name);

        // Load expected watch plan
        let watch_plan = load_expected_watch_plan(spec_name);

        // Modify transform spec
        let aggregate_tx = full_spec
            .data
            .get_mut( 0)
            .unwrap()
            .transform
            .get_mut(2)
            .unwrap();
        if let TransformSpec::Aggregate(aggregate_tx) = aggregate_tx {
            aggregate_tx.ops = Some(vec![agg.clone()]);
        } else {
            panic!("Unexpected transform")
        }

        // Build name for saved images
        let output_name = format!("{}_agg_{:?}", spec_name, agg);

        TOKIO_RUNTIME.block_on(check_spec_sequence(
            full_spec,
            full_updates,
            watch_plan,
            &output_name,
            0.001,
        ));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_image_comparison_window {
    use super::*;

    use vegafusion_core::spec::transform::window::WindowTransformOpSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest]
    fn test_image_comparison(
        #[values(
            "count",
            "max",
            "min",
            "mean",
            "average",
            "row_number",
            "rank",
            "dense_rank",
            "percent_rank",
            "cume_dist",
            "first_value",
            "last_value"
        )]
        op_name: &str,

        #[values("custom/cumulative_running_window")] spec_name: &str,
    ) {
        println!("op: {}", op_name);
        // Load spec
        let mut full_spec = load_spec(spec_name);

        // Load updates
        let full_updates = load_updates(spec_name);

        // Load expected watch plan
        let watch_plan = load_expected_watch_plan(spec_name);

        // Window
        let window_tx = full_spec
            .data
            .get_mut(0)
            .unwrap()
            .transform
            .get_mut(2)
            .unwrap();

        if let TransformSpec::Window(window_tx) = window_tx {
            let op: WindowTransformOpSpec =
                serde_json::from_str(&format!("\"{}\"", op_name)).unwrap();
            window_tx.ops = vec![op];
        } else {
            panic!("Unexpected transform")
        }

        // Build name for saved images
        let output_name = format!("{}_{}", spec_name, op_name);

        TOKIO_RUNTIME.block_on(check_spec_sequence(
            full_spec,
            full_updates,
            watch_plan,
            &output_name,
            0.001,
        ));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_pre_transform_inline {
    use super::*;
    use crate::util::datasets::vega_json_dataset_async;
    use vegafusion_core::proto::gen::pretransform::PreTransformInlineDataset;

    #[tokio::test]
    async fn test() {
        initialize();

        let vegajs_runtime = vegajs_runtime();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

        // Get timezone
        let local_tz = vegajs_runtime.nodejs_runtime.local_timezone().unwrap();

        // Load specs
        let full_spec = load_spec("pre_transform/imdb_histogram");
        let inline_spec = load_spec("pre_transform/imdb_histogram_inline");

        // Load csv file as inline dataset
        let movies_table = vega_json_dataset_async("movies").await;
        let inline_datasets = vec![PreTransformInlineDataset {
            name: "movies".to_string(),
            table: movies_table.to_ipc_bytes().unwrap(),
        }];

        // Pre-transform specs
        let opts = PreTransformSpecOpts {
            row_limit: None,
            inline_datasets,
        };
        let request = PreTransformSpecRequest {
            spec: serde_json::to_string(&inline_spec).unwrap(),
            local_tz,
            output_tz: None,
            opts: Some(opts),
        };
        let response = runtime.pre_transform_spec_request(request).await.unwrap();

        let pre_transform_spec: ChartSpec = match response.result.unwrap() {
            pre_transform_spec_result::Result::Error(_) => {
                panic!("Pre-transform error")
            }
            pre_transform_spec_result::Result::Response(response) => {
                // println!("Warnings: {:#?}", response.warnings);
                serde_json::from_str(&response.spec).unwrap()
            }
        };

        // println!(
        //     "pre-transformed: {}",
        //     serde_json::to_string_pretty(&pre_transform_spec).unwrap()
        // );

        let full_image = vegajs_runtime
            .export_spec_single(&full_spec, ExportImageFormat::Png)
            .unwrap();
        let pre_transformed_image = vegajs_runtime
            .export_spec_single(&pre_transform_spec, ExportImageFormat::Png)
            .unwrap();

        let png_name = "pre_transform-imbd_histogram";
        full_image
            .save(
                &format!("{}/tests/output/{}_full.png", crate_dir(), png_name),
                true,
            )
            .unwrap();

        pre_transformed_image
            .save(
                &format!("{}/tests/output/{}_pretransform.png", crate_dir(), png_name),
                true,
            )
            .unwrap();

        let (difference, diff_img) = full_image.compare(&pre_transformed_image).unwrap();
        if difference > 0.001 {
            println!("difference: {}", difference);
            if let Some(diff_img) = diff_img {
                let diff_path = format!(
                    "{}/tests/output/{}_pretransform_diff.png",
                    crate_dir(),
                    png_name
                );
                fs::write(&diff_path, diff_img).unwrap();
                panic!(
                    "Found difference in exported images.\nDiff written to {}",
                    diff_path
                )
            }
        }
    }
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

fn load_spec(spec_name: &str) -> ChartSpec {
    // Load spec
    let spec_path = format!("{}/tests/specs/{}.vg.json", crate_dir(), spec_name);
    let spec_str = fs::read_to_string(spec_path).unwrap();
    serde_json::from_str(&spec_str).unwrap()
}

fn load_updates(spec_name: &str) -> Vec<ExportUpdateBatch> {
    let updates_path = format!("{}/tests/specs/{}.updates.json", crate_dir(), spec_name);
    let updates_path = std::path::Path::new(&updates_path);

    if updates_path.exists() {
        let updates_str = fs::read_to_string(updates_path).unwrap();
        serde_json::from_str(&updates_str).unwrap()
    } else {
        Vec::new()
    }
}

fn load_expected_watch_plan(spec_name: &str) -> WatchPlan {
    let watch_plan_path = format!("{}/tests/specs/{}.comm_plan.json", crate_dir(), spec_name);
    let watch_plan_path = std::path::Path::new(&watch_plan_path);

    let comm_plan_str = fs::read_to_string(watch_plan_path).unwrap();
    serde_json::from_str(&comm_plan_str).unwrap()
}

#[allow(dead_code)]
fn write_updated_watch_plan(spec_name: &str, plan: &WatchPlan) {
    let watch_plan_path = format!("{}/tests/specs/{}.comm_plan.json", crate_dir(), spec_name);
    let watch_plan_path = std::path::Path::new(&watch_plan_path);
    fs::write(watch_plan_path, serde_json::to_string_pretty(plan).unwrap()).unwrap()
}

async fn check_spec_sequence_from_files(spec_name: &str, tolerance: f64) {
    initialize();

    // Load spec
    let full_spec = load_spec(spec_name);

    // Load updates
    let full_updates = load_updates(spec_name);

    // Load expected watch plan
    let watch_plan = load_expected_watch_plan(spec_name);

    check_spec_sequence(full_spec, full_updates, watch_plan, spec_name, tolerance).await
}

async fn check_pre_transform_spec_from_files(spec_name: &str, tolerance: f64) {
    initialize();

    // Load spec
    let full_spec = load_spec(spec_name);
    println!("{:#?}", full_spec);

    let vegajs_runtime = vegajs_runtime();

    // Initialize task graph runtime
    let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

    // Get timezone
    let local_tz = vegajs_runtime.nodejs_runtime.local_timezone().unwrap();

    // Pre-transform specs
    let opts = PreTransformSpecOpts {
        row_limit: None,
        inline_datasets: vec![],
    };
    let request = PreTransformSpecRequest {
        spec: serde_json::to_string(&full_spec).unwrap(),
        local_tz,
        output_tz: None,
        opts: Some(opts),
    };
    let response = runtime.pre_transform_spec_request(request).await.unwrap();

    let pre_transform_spec: ChartSpec = match response.result.unwrap() {
        pre_transform_spec_result::Result::Error(_) => {
            panic!("Pre-transform error")
        }
        pre_transform_spec_result::Result::Response(response) => {
            // println!("Warnings: {:#?}", response.warnings);
            serde_json::from_str(&response.spec).unwrap()
        }
    };

    // println!(
    //     "pre-transformed: {}",
    //     serde_json::to_string_pretty(&pre_transform_spec).unwrap()
    // );

    let full_image = vegajs_runtime
        .export_spec_single(&full_spec, ExportImageFormat::Png)
        .unwrap();
    let pre_transformed_image = vegajs_runtime
        .export_spec_single(&pre_transform_spec, ExportImageFormat::Png)
        .unwrap();

    let png_name = spec_name.replace('/', "-");
    full_image
        .save(
            &format!("{}/tests/output/{}_full.png", crate_dir(), png_name),
            true,
        )
        .unwrap();

    pre_transformed_image
        .save(
            &format!("{}/tests/output/{}_pretransform.png", crate_dir(), png_name),
            true,
        )
        .unwrap();

    let (difference, diff_img) = full_image.compare(&pre_transformed_image).unwrap();
    if difference > tolerance {
        println!("difference: {}", difference);
        if let Some(diff_img) = diff_img {
            let diff_path = format!(
                "{}/tests/output/{}_pretransform_diff.png",
                crate_dir(),
                png_name
            );
            fs::write(&diff_path, diff_img).unwrap();
            panic!(
                "Found difference in exported images.\nDiff written to {}",
                diff_path
            )
        }
    }
}

async fn check_spec_sequence(
    full_spec: ChartSpec,
    full_updates: Vec<ExportUpdateBatch>,
    watch_plan: WatchPlan,
    spec_name: &str,
    tolerance: f64,
) {
    // Initialize runtime
    let vegajs_runtime = vegajs_runtime();
    let local_tz = vegajs_runtime.nodejs_runtime.local_timezone().unwrap();
    let tz_config = TzConfig {
        local_tz,
        default_input_tz: None,
    };

    let spec_plan = SpecPlan::try_new(&full_spec, &Default::default()).unwrap();

    let task_scope = spec_plan.server_spec.to_task_scope().unwrap();

    // println!("task_scope: {:#?}", task_scope);

    // println!(
    //     "client_spec: {}",
    //     serde_json::to_string_pretty(&spec_plan.client_spec).unwrap()
    // );
    // println!(
    //     "server_spec: {}",
    //     serde_json::to_string_pretty(&spec_plan.server_spec).unwrap()
    // );
    //
    // println!(
    //     "comm_plan:\n---\n{}\n---",
    //     serde_json::to_string_pretty(&WatchPlan::from(spec_plan.comm_plan.clone())).unwrap()
    // );

    // Build task graph
    let tasks = spec_plan
        .server_spec
        .to_tasks(&tz_config, &Default::default())
        .unwrap();
    let mut task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
    let task_graph_mapping = task_graph.build_mapping();

    // Collect watch variables and node indices
    let watch_vars = spec_plan.comm_plan.server_to_client.clone();
    let watch_indices: Vec<_> = watch_vars
        .iter()
        .map(|var| task_graph_mapping.get(var).unwrap())
        .collect();

    // Initialize task graph runtime
    let runtime = TaskGraphRuntime::new(Some(16), Some(1024_i32.pow(3) as usize));

    // Extract the initial values of all of the variables that should be sent from the
    // server to the client

    // println!("comm_plan: {:#?}", comm_plan);

    let mut init = Vec::new();
    for var in &spec_plan.comm_plan.server_to_client {
        let node_index = task_graph_mapping.get(var).unwrap();
        let value = runtime
            .get_node_value(Arc::new(task_graph.clone()), node_index, Default::default())
            .await
            .expect("Failed to get node value");

        init.push(ExportUpdate {
            namespace: ExportUpdateNamespace::try_from(var.0.namespace()).unwrap(),
            name: var.0.name.clone(),
            scope: var.1.clone(),
            value: value.to_json().unwrap(),
        });
    }

    // println!("init: {:#?}", init);

    // Build watches for all of the variables that should be sent from the client to the
    // server
    let watches: Vec<_> = spec_plan
        .comm_plan
        .client_to_server
        .iter()
        .map(|t| Watch::try_from(t.clone()).unwrap())
        .collect();

    // Export sequence with full spec
    let export_sequence_results = vegajs_runtime
        .export_spec_sequence(
            &full_spec,
            ExportImageFormat::Png,
            Vec::new(),
            full_updates.clone(),
            watches,
        )
        .unwrap();

    // Save exported PNGs
    let png_name = spec_name.replace('/', "-");
    for (i, (export_image, _)) in export_sequence_results.iter().enumerate() {
        export_image
            .save(
                &format!("{}/tests/output/{}_full{}", crate_dir(), png_name, i),
                true,
            )
            .unwrap();
    }

    // Update graph with client-to-server watches, and collect values to update the client with
    let mut server_to_client_value_batches: Vec<HashMap<ScopedVariable, TaskValue>> = Vec::new();
    for (_, watch_values) in export_sequence_results.iter().skip(1) {
        // Update graph
        for watch_value in watch_values {
            let variable = watch_value.watch.to_scoped_variable();
            let value = match &watch_value.watch.namespace {
                WatchNamespace::Signal => {
                    TaskValue::Scalar(ScalarValue::from_json(&watch_value.value).unwrap())
                }
                WatchNamespace::Data => {
                    TaskValue::Table(VegaFusionTable::from_json(&watch_value.value, 1024).unwrap())
                }
            };

            let index = task_graph_mapping.get(&variable).unwrap().node_index as usize;
            task_graph.update_value(index, value).unwrap();
        }

        // Get updated server to client values
        let mut server_to_client_value_batch = HashMap::new();
        for (var, node_index) in watch_vars.iter().zip(&watch_indices) {
            let value = runtime
                .get_node_value(Arc::new(task_graph.clone()), node_index, Default::default())
                .await
                .unwrap();

            server_to_client_value_batch.insert(var.clone(), value);
        }

        server_to_client_value_batches.push(server_to_client_value_batch);
    }

    // Merge the original updates with the original batch updates
    let planned_spec_updates: Vec<_> = full_updates
        .iter()
        .zip(server_to_client_value_batches)
        .map(|(full_export_updates, server_to_client_values)| {
            let server_to_clients_export_updates: Vec<_> = server_to_client_values
                .iter()
                .map(|(scoped_var, value)| {
                    let json_value = match value {
                        TaskValue::Scalar(value) => value.to_json().unwrap(),
                        TaskValue::Table(value) => value.to_json(),
                    };
                    ExportUpdate {
                        namespace: ExportUpdateNamespace::try_from(scoped_var.0.namespace())
                            .unwrap(),
                        name: scoped_var.0.name.clone(),
                        scope: scoped_var.1.clone(),
                        value: json_value,
                    }
                })
                .collect();

            let mut total_updates = Vec::new();

            total_updates.extend(server_to_clients_export_updates);
            total_updates.extend(full_export_updates.clone());

            // export_updates
            total_updates
        })
        .collect();

    // Export the planned client spec with updates from task graph

    // Compare exported images
    for (i, server_img) in vegajs_runtime
        .export_spec_sequence(
            &spec_plan.client_spec,
            ExportImageFormat::Png,
            init,
            planned_spec_updates,
            Default::default(),
        )
        .unwrap()
        .into_iter()
        .map(|(img, _)| img)
        .enumerate()
    {
        server_img
            .save(
                &format!("{}/tests/output/{}_planned{}", crate_dir(), png_name, i),
                true,
            )
            .unwrap();
        let (full_img, _) = &export_sequence_results[i];

        let (difference, diff_img) = full_img.compare(&server_img).unwrap();
        if difference > tolerance {
            println!("difference: {}", difference);
            if let Some(diff_img) = diff_img {
                let diff_path = format!("{}/tests/output/{}_diff{}.png", crate_dir(), png_name, i);
                fs::write(&diff_path, diff_img).unwrap();
                panic!(
                    "Found difference in exported images.\nDiff written to {}",
                    diff_path
                )
            }
        }
    }

    // Check for expected comm plan
    assert_eq!(watch_plan, WatchPlan::from(spec_plan.comm_plan))
}
