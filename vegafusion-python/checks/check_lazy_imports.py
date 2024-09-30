import sys
import vegafusion as vf

for mod in ["polars", "pandas", "pyarrow", "duckdb", "altair"]:
    assert mod not in sys.modules, f"{mod} module should be imported lazily"
