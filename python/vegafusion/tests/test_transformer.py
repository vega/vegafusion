# VegaFusion
# Copyright (C) 2022, Jon Mease
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import pandas as pd
import pyarrow as pa
from decimal import Decimal
from vegafusion.transformer import to_arrow_table


def test_to_arrow_expands_categoricals():
    # Build DataFrame with one categorical column
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["One", "One", "Two"]
    })
    df["b"] = df["b"].astype("category")
    assert isinstance(df["b"].dtype, pd.CategoricalDtype)

    # Convert to pyarrow table
    pa_table = to_arrow_table(df)

    # Check that pyarrow type is String (not Dictionary)
    b_type = pa_table.column("b").type
    assert b_type == pa.string()


def test_to_table_converts_decimals():
    # Build DataFrame with one Decimal column
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [Decimal("3.12"), Decimal("4.9"), Decimal("6")]
    })
    assert df["b"].dtype.kind == "O"

    # Convert to pyarrow table
    pa_table = to_arrow_table(df)

    # Check that pyarrow type is float64 (not Decimal128)
    b_type = pa_table.column("b").type
    assert b_type == pa.float64()


def test_to_table_with_mixed_string_int_column():
    # Build DataFrame with one Decimal column
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["A", "B", 3]
    })
    assert df["b"].dtype.kind == "O"

    # Convert to pyarrow table
    pa_table = to_arrow_table(df)

    # Check that pyarrow type is float64 (not Decimal128)
    b_type = pa_table.column("b").type
    assert b_type == pa.string()


def test_to_table_with_all_conversions():
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["One", "One", "Two"],
        "c": [Decimal("3.12"), Decimal("4.9"), Decimal("6")],
        "d": ["A", "B", 3]
    })
    df["b"] = df["b"].astype("category")

    # Check initial pandas column types
    assert isinstance(df["b"].dtype, pd.CategoricalDtype)
    assert df["c"].dtype.kind == "O"
    assert df["d"].dtype.kind == "O"

    # Convert to pyarrow table
    pa_table = to_arrow_table(df)

    # Check pyarrow types
    assert pa_table.column("b").type == pa.string()
    assert pa_table.column("c").type == pa.float64()
    assert pa_table.column("d").type == pa.string()
