from typing import Iterable
from math import floor
import pandas as pd
import pyarrow as pa
from .datasource import Datasource


class PandasDatasource(Datasource):
    def __init__(self, df: pd.DataFrame, sample_size: int = 1000, batch_size: int = 8096):
        fields = []
        casts = {}
        sample_stride = max(1, floor(len(df) / sample_size))

        # Shallow copy and add named index levels as columns
        # (this is faster that df.reset_index, which seems to perform a deep copy)
        df = df.copy(deep=False)
        for i, index_name in enumerate(getattr(df.index, "names", [])):
            if isinstance(index_name, str):
                df[index_name] = df.index.get_level_values(i)
        df.reset_index(drop=True, inplace=True)

        for col, pd_type in df.dtypes.items():
            if not isinstance(col, str):
                continue
            try:
                # We will expand categoricals (not yet supported in VegaFusion)
                if isinstance(pd_type, pd.CategoricalDtype):
                    cat = df[col].cat
                    field = pa.Schema.from_pandas(pd.DataFrame({col: cat.categories})).field(col)
                else:
                    candidate_schema = pa.Schema.from_pandas(df.iloc[::sample_stride][[col]])
                    field = candidate_schema.field(col)

                # Convert Decimal columns to float
                if isinstance(field.type, (pa.Decimal128Type, pa.Decimal256Type)):
                    field = pa.field(col, pa.float64())
                    casts[col] = "float64"

                # Get inferred pyarrow type
                fields.append(field)

            except pa.ArrowTypeError:
                if pd_type.kind == "O":
                    fields.append(pa.field(col, pa.string()))
                    casts[col] = str
                else:
                    raise
        self._df = df
        self._schema = pa.schema(fields)
        self._casts = casts
        self._batch_size = batch_size

    def schema(self) -> pa.Schema:
        return self._schema

    def fetch(self, columns: Iterable[str]) -> pa.Table:
        projected = self._df[columns].copy(deep=False)

        for col, pd_type in projected.dtypes.items():
            # Handle cases that need to happen before conversion to pyarrow
            if col in self._casts:
                projected[col] = projected[col].astype(self._casts[col])

            # Handle expanding categoricals
            if isinstance(pd_type, pd.CategoricalDtype):
                cat = projected[col].cat
                projected[col] = cat.categories[cat.codes]

        table = pa.Table.from_pandas(projected, preserve_index=False)

        # Split into batches of desired size
        if self._batch_size is None:
            return table
        else:
            # Resize batches
            batches = table.to_batches(self._batch_size)
            return pa.Table.from_batches(batches, table.schema)
