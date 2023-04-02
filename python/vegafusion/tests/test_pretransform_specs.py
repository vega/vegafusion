from pathlib import Path
import json
import vegafusion as vf
from vl_convert import vega_to_png

from io import BytesIO
import pyarrow as pa
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
            if file.name not in [
                # Undefined aggregate column
                "trellis_area_seattle.vg.json"
            ]:
                cases.append((category, file.name))

    return cases


def maybe_skip(category, name):
    if (category, name) in [
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
        ("vegalite", "errorband_2d_vertical_borders.vg.json"),
        ("vegalite", "errorbar_2d_vertical_ticks.vg.json"),
        ("vegalite", "layer_line_errorband_ci.vg.json"),
        ("vegalite", "errorband_2d_horizontal_color_encoding.vg.json"),
        ("vegalite", "layer_line_errorband_2d_horizontal_borders_strokedash.vg.json"),
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


def test_pretransform_extract():
    spec_file = spec_dir / "vegalite" / "rect_binned_heatmap.vg.json"
    spec = json.loads(spec_file.read_text("utf8"))

    vf.runtime.set_connection("datafusion")
    (transformed, datasets, warnings) = vf.runtime.pre_transform_extract(spec, "UTC")

    assert len(warnings) == 0
    assert len(datasets) == 1

    (name, scope, table)= datasets[0]
    assert name == "source_0"
    assert scope == []
    assert isinstance(table, pa.Table)
    assert table.shape == (379, 5)
