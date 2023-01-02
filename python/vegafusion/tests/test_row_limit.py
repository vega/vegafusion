import pytest
import vegafusion as vf
import altair as alt
from altair.utils.execeval import eval_block
from pathlib import Path
here = Path(__file__).parent
altair_mocks_dir = here / "altair_mocks"


def test_row_limit():
    mock_path = altair_mocks_dir / "scatter" / "bubble_plot" / "mock.py"
    mock_src = mock_path.read_text("utf8")
    chart = eval_block(mock_src)

    # Dataset has 406 rows (before filtering out nulls) and this chart is not aggregated.
    # Limit of 500 rows should be fine
    with vf.enable(row_limit=500):
        chart._repr_mimebundle_()

    # Limit of 300 rows should raise RowLimitError
    with vf.enable(row_limit=300):
        with pytest.raises(vf.RowLimitError):
            chart._repr_mimebundle_()

    # Adding an aggregation should allow row limit to be much lower
    chart = chart.encode(
        alt.X("Horsepower", bin=True),
        alt.Y("Miles_per_Gallon", bin=True),
        alt.Size("count()")
    )
    with vf.enable(row_limit=50):
        chart._repr_mimebundle_()
