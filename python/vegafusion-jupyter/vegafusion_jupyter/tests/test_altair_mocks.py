from pathlib import Path
import jupytext
import io
import tempfile
from subprocess import Popen
import chromedriver_binary
import pytest
from selenium.webdriver import ActionChains
from selenium import webdriver
import time
from io import BytesIO
from skimage.io import imread
from skimage.metrics import structural_similarity as ssim
import json
import shutil

here = Path(__file__).parent
altair_mocks_dir = here / "altair_mocks"
temp_notebooks_dir = here / "temp_notebooks"
temp_screenshots_dir = here / "temp_screenshot"

altair_markdown_template = r"""
```python
import altair as alt
alt.renderers.enable('default', embed_options={'actions': False});
```

```python
{code}
```

```python
assert(alt.renderers.active == "default")
assert(alt.data_transformers.active == 'default')
```
"""

vegafusion_arrow_markdown_template = r"""
```python
import altair as alt
import vegafusion_jupyter
```

```python
{code}
```

```python
assert(alt.renderers.active == "vegafusion")
assert(alt.data_transformers.active == 'vegafusion-arrow')
```
"""

vegafusion_default_markdown_template = r"""
```python
import altair as alt
import vegafusion_jupyter
alt.data_transformers.enable("default");
```

```python
{code}
```

```python
assert(alt.renderers.active == "vegafusion")
assert(alt.data_transformers.active == 'default')
```
"""


@pytest.mark.parametrize(
    "mock_name", [
        "area/cumulative_count",
        "area/density_facet",
        "area/gradient",
        "area/horizon_graph",
        "area/layered",
        "area/normalized_stacked",
        "area/density_stack",
        "area/trellis",
        "area/trellis_sort_array",
        "area/streamgraph",
        "bar/with_highlighted_bar",
        "bar/with_labels",
        "bar/with_line_at_mean",
        "bar/with_line_on_dual_axis",
        "bar/with_rolling_mean",
        "bar/with_rounded_edges",
        "bar/and_tick_chart",
        "bar/percentage_of_total",
        "bar/trellis_compact",
        "bar/diverging_stacked",
        "bar/grouped",
        "bar/horizontal",
        "bar/horizontal_grouped",
        "bar/horizontal_stacked",
        "bar/normalized_stacked",
        "bar/sorted",
        "bar/stacked",
        "bar/stacked_with_sorted_segments",
        "bar/stacked_with_text_overlay",
        "bar/trellis_stacked",
        "bar/trellis_stacked",
        "bar/with_negative_values",
        "bar/layered",
        "casestudy/co2_concentration",
        "casestudy/gapminder_bubble_plot",
        "casestudy/iowa_electricity",
        "casestudy/natural_disasters",
        "casestudy/top_k_with_others",
        "casestudy/wheat_wages",
        "casestudy/window_rank",
        "casestudy/airports",
        "casestudy/us_state_capitals",
        "casestudy/falkensee",
        "casestudy/us_employment",
        "casestudy/top_k_items",
        "casestudy/top_k_letters",
        "casestudy/isotype",
        "circular/donut",
        "circular/pie",
        "circular/pie_with_labels",
        "circular/radial",
        "circular/pacman",
        "histogram/with_a_global_mean_overlay",
        "histogram/layered",
        "histogram/trellis",
        "interactive/selection_layer_bar_month",
        "interactive/area-interval_selection",
        "interactive/layered_crossfilter",
        "interactive/scatter_with_histogram",
        "interactive/select_detail",
        "interactive/scatter_plot",
        "interactive/brush",
        "interactive/multiline_tooltip",
        "interactive/scatter_linked_brush",
        "interactive/casestudy-us_population_pyramid_over_time",
        "interactive/multiline_highlight",
        "interactive/scatter-with_minimap",
        "interactive/select_mark_area",
        "interactive/legend",
        "interactive/cross_highlight",
        "interactive/selection_histogram",
        "interactive/scatter-with_linked_table",
        "interactive/scatter_with_layered_histogram",
        "interactive/casestudy-seattle_weather_interactive",
        "interactive/casestudy-us_population_over_time",
        "line/bump_chart",
        "line/filled_step_chart",
        "line/with_cumsum",
        "line/with_logarithmic_scale",
        "line/percent_axis",
        "line/with_points",
        "line/with_generator",
        "line/slope_graph",
        "line/slope_graph2",
        "line/step_chart",
        "line/layer_line_color_rule",
        "line/multi_series",
        "line/with_ci",
        "line/trail_marker",
        "line/with_datum",
        "line/with_color_datum",
        "maps/choropleth",
        "maps/choropleth_repeat",
        "maps/us_incomebrackets_by_state_facet",
        "maps/world",
        "maps/world_projections",
        "maps/airports_count",
        "other/bar_chart_with_highlighted_segment",
        "other/beckers_barley_wrapped_facet",
        "other/boxplot",
        "other/comet_chart",
        "other/errorbars_with_std",
        "other/scatter_marginal_hist",
        "other/gantt_chart",
        "other/isotype_grid",
        "other/layered_chart_with_dual_axis",
        "other/ridgeline_plot",
        "other/stem_and_leaf",
        "other/layered_heatmap_text",
        "other/candlestick_chart",
        "other/multiple_marks",
        "other/hexbins",
        "other/wilkinson_dot_plot",
        "other/binned_heatmap",
        "scatter/binned",
        "scatter/bubble_plot",
        "scatter/connected",
        "scatter/dot_dash_plot",
        "scatter/multifeature",
        "scatter/poly_fit_regression",
        "scatter/qq",
        "scatter/matrix",
        "scatter/with_lowess",
        "scatter/with_errorbars",
        "scatter/with_labels",
        "scatter/table_bubble_plot_github",
        "scatter/trellis",
        "scatter/wind_vector_map",
        "scatter/with_rolling_mean",
        "simple/stacked_bar_chart",
        "simple/bar_chart",
        "simple/heatmap",
        "simple/line_chart",
        "simple/scatter_tooltips",
        "simple/strip_chart",

        # # Not yet supported
        # # -----------------
        # # Need to support facet data as source
        # "bar/with_error_bars",
        # "casestudy/beckers_barley_trellis_plot",
        #
        # # Need to pick up data set use in a signal in a "sequence" transform
        # "casestudy/anscombe_plot"
        # "casestudy/us_population_over_time_facet",
        #
        # # ci interval uses random number generator and is not deterministic
        # "other/errorbars_with_ci",
        # "other/sorted_error_bars_with_ci",
        #
        # # Need support for bin `extent` as signal expression
        # "interactive/histogram-responsive",
        #
        # # Lines messed up
        # "other/normed_parallel_coordinates",
        # "other/parallel_coordinates",
        #
        # # US and Europe Violins missing
        # "other/violin_plot",
        #
        #
        # # Tooltips not supported
        # "interactive/scatter-href",
        # "interactive/other-image_tooltip",
        # "interactive/casestudy-weather_heatmap",
        # "interactive/casestudy-airport_connections",
        #
        # # Unsupported Expression functions
        # "casestudy/london_tube",  # indexof and substring not supported
        # "casestudy/one_dot_per_zipcode",  # substring
        # "other/ranged_dot_plot",  # indexof
        # "casestudy/isotype_emoji", # Unsupported use of datum expression in literal object member lookup
        # "scatter/stripplot",  # random()
    ])
def test_altair_mock(mock_name):

    # Build Jupytext markdown text containing the mock's code
    mock_path = altair_mocks_dir / mock_name / "mock.py"
    actions = load_actions(mock_name)

    mock_code = mock_path.read_text()
    altair_markdown = altair_markdown_template.replace("{code}", mock_code)
    vegafusion_arrow_markdown = vegafusion_arrow_markdown_template.replace("{code}", mock_code)
    vegafusion_default_markdown = vegafusion_default_markdown_template.replace("{code}", mock_code)

    # Use jupytext to convert markdown to an ipynb file
    altair_notebook = jupytext.read(io.StringIO(altair_markdown), fmt="markdown")
    vegafusion_arrow_notebook = jupytext.read(io.StringIO(vegafusion_arrow_markdown), fmt="markdown")
    vegafusion_default_notebook = jupytext.read(io.StringIO(vegafusion_default_markdown), fmt="markdown")

    # Initialize notebooks and screenshots to empty directories
    shutil.rmtree(temp_notebooks_dir, ignore_errors=True)
    temp_notebooks_dir.mkdir(parents=True, exist_ok=True)

    shutil.rmtree(temp_screenshots_dir, ignore_errors=True)
    temp_screenshots_dir.mkdir(parents=True, exist_ok=True)

    # Create selenium Chrome instance
    chrome_driver = webdriver.Chrome()
    chrome_driver.set_window_size(800, 800)

    # Launch Voila server
    voila_proc = Popen(["voila", "--no-browser", "--enable_nbextensions=True"], cwd=temp_notebooks_dir)
    time.sleep(2)

    try:
        altair_imgs = export_image_sequence(chrome_driver, altair_notebook, actions)
        vegafusion_arrow_imgs = export_image_sequence(chrome_driver, vegafusion_arrow_notebook, actions)
        vegafusion_default_imgs = export_image_sequence(chrome_driver, vegafusion_default_notebook, actions)

        for i in range(len(altair_imgs)):
            altair_img = altair_imgs[i]
            vegafusion_arrow_img = vegafusion_arrow_imgs[i]
            vegafusion_default_img = vegafusion_default_imgs[i]

            assert altair_img.shape == vegafusion_arrow_img.shape, "Size mismatch with Arrow data transformer"
            assert altair_img.shape == vegafusion_default_img.shape, "Size mismatch with default data transformer"

            similarity_arrow_value = ssim(altair_img, vegafusion_arrow_img, channel_axis=2)
            similarity_default_value = ssim(altair_img, vegafusion_default_img, channel_axis=2)

            print(f"({i}) {similarity_arrow_value=}")
            print(f"({i}) {similarity_default_value=}")

            assert similarity_arrow_value > 0.995, f"Similarity failed with Arrow data transformer on image {i}"
            assert similarity_default_value > 0.995, f"Similarity failed with default data transformer on image {i}"

    finally:
        voila_proc.kill()
        chrome_driver.close()


def load_actions(mock_name):
    actions_path = altair_mocks_dir / mock_name / "actions.json"
    if actions_path.exists():
        return json.loads(actions_path.read_text())
    else:
        return [{"type": "snapshot"}]


def export_image_sequence(
        chrome_driver: webdriver.Chrome,
        notebook: jupytext.jupytext.NotebookNode,
        actions,
        voila_url_base: str = "http://localhost:8866/voila/render/",
):
    imgs = []

    with tempfile.NamedTemporaryFile(mode="wt", dir=temp_notebooks_dir, suffix=".ipynb") as f:
        jupytext.write(notebook, f, fmt="ipynb")
        f.file.flush()

        temp_file_path = Path(f.name)

        # Server file with voila
        url = voila_url_base.rstrip("/") + "/" + temp_file_path.name

        # Open url with selenium
        chrome_driver.get(url)
        time.sleep(4)

        # Remove padding, margins, and standardize line height.
        css = ("body, .jp-Cell, .jp-Notebook, .jupyter-widgets, .jp-RenderedHTMLCommon "
               "{margin: 0 !important; padding: 0 !important; line-height: 1.3 !important;}")
        script = 'document.styleSheets[0].insertRule("' + css + '", 0 )'
        chrome_driver.execute_script(script)

        # Get canvas element (the canvas that Vega renders to)
        canvas = chrome_driver.find_element_by_xpath("//canvas")

        # Process actions
        chain = ActionChains(chrome_driver)
        for i, action in enumerate(actions):
            action_type = action["type"]
            if action_type in ("snapshot", "screenshot"):
                chain.perform()
                time.sleep(0.5)

                img_path = (temp_screenshots_dir / (temp_file_path.name + f"_{i}.png")).as_posix();
                if action_type == "snapshot":
                    img_bytes = canvas.screenshot_as_png
                    # Write to file for debugging
                    canvas.screenshot(img_path)
                else:
                    img_bytes = chrome_driver.get_screenshot_as_png()
                    chrome_driver.save_screenshot(img_path)

                # Get png representation in binary (bytes) from driver
                # convert this into a 3D numpy image
                img = imread(BytesIO(img_bytes))
                imgs.append(img)

                # Reset chain
                chain = ActionChains(chrome_driver)

            elif action_type == "move_to":
                coords = action["coords"]
                chain = chain.move_to_element_with_offset(canvas, coords[0], coords[1])
            elif action_type == "move_by":
                coords = action["coords"]
                chain = chain.move_by_offset(coords[0], coords[1])
            elif action_type == "click_and_hold":
                chain = chain.click_and_hold()
            elif action_type == "click":
                chain = chain.click()
            elif action_type == "double_click":
                chain = chain.double_click()
            elif action_type == "release":
                chain = chain.release()
            else:
                raise ValueError(f"Invalid action type {action_type}")

    return imgs
