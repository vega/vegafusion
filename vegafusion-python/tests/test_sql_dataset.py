import duckdb
import pytest

from vegafusion.dataset.duckdb import DuckDbDataset


def test_sql_dataset_dfi():
    try:
        import pyarrow.interchange as pi
    except ImportError:
        pytest.skip("DataFrame interface protocol requires pyarrow 11.0.0 or later")

    rel = duckdb.query("SELECT 1 as a")
    dataset = DuckDbDataset(rel)
    table = pi.from_dataframe(dataset)
    assert table.num_rows == 1
    assert table.column_names == ["a"]
