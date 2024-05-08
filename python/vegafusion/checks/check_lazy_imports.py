import sys
import vegafusion as vf
import vegafusion_embed

for mod in ["polars", "pandas", "pyarrow", "duckdb"]:
    assert mod not in sys.modules
