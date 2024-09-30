import pandas as pd
import pyarrow as pa
from vegafusion.datasource import PandasDatasource

def test_mixed_col_type_inference():
    df = pd.DataFrame({
        'a': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'],
        'b': [28, 55, 43, 91, 81, 53, 19, 87, 52],
        'c': [28, 55, 43, 91, 81, 53, 19, 87, 52],
        'd': [28, 55, 43, 91, 81, 53, 19, 87, 52],
        'e': [28, 55, 43, 91, 81, 53, 19, 87, 52],
        'f': [28, 55, 43, 91, '81.5', 53, 19, 87, 52],
    })
    datasource = PandasDatasource(df)

    expected_schema = pa.schema([
        pa.field("a", pa.string()),
        pa.field("b", pa.int64()),
        pa.field("c", pa.int64()),
        pa.field("d", pa.int64()),
        pa.field("e", pa.int64()),
        pa.field("f", pa.string()),
    ])

    assert datasource.schema() == expected_schema
