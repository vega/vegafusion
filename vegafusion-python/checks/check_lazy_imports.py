import sys
from pathlib import Path

root = Path(__file__).parent.parent.parent


if __name__ == "__main__":
    # Make sure the prominant dependencies are not loaded on import
    import vegafusion as vf  # noqa: F401

    for mod in ["polars", "pandas", "pyarrow", "duckdb", "altair"]:
        assert mod not in sys.modules, f"{mod} module should be imported lazily"

    # Create an altair chart with polars and check that pandas and pyarrow are
    # not loaded
    import altair as alt
    import polars as pl

    cars = pl.read_json(
        root / "vegafusion-runtime/tests/util/vegajs_runtime/data/cars.json"
    )

    # Build a histogram of horsepower
    chart = (
        alt.Chart(cars)
        .mark_bar()
        .encode(
            alt.X("Horsepower:Q", bin=True),
            y="count()",
        )
    )

    # Check that the transformed data is a polars DataFrame
    transformed = chart.transformed_data()
    assert isinstance(transformed, pl.DataFrame)
    assert len(transformed["bin_maxbins_10_Horsepower"]) == 10

    # Do a full pre-transform of the spec
    transformed_spec = chart.to_dict(format="vega")
    assert isinstance(transformed_spec, dict)
    assert "data" in transformed_spec

    # Make sure that pandas and pyarrow were not loaded when using polars
    for mod in ["pandas", "pyarrow", "duckdb"]:
        assert mod not in sys.modules, f"{mod} module should be imported lazily"
