import sqlite3
import pandas as pd
from pathlib import Path

here = Path(__file__).parent

if __name__ == "__main__":
    # Delete and open database file
    db_file = here / "data" / "vega_datasets.db"
    db_file.unlink(True)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_file)

    # Download datasets
    dataset_root = "https://raw.githubusercontent.com/vega/vega-datasets/next/data/"

    for dataset_name in [
        "movies.json",
        "seattle-weather.csv",
        "stocks.csv",
    ]:
        full_url = dataset_root + dataset_name

        # Read dataframe
        if dataset_name.endswith(".json"):
            df = pd.read_json(full_url)
            name = dataset_name.rstrip(".json")
        elif dataset_name.endswith(".csv"):
            df = pd.read_csv(full_url)
            name = dataset_name.rstrip(".csv")
        else:
            print(f"Unknown format for file {full_url}")
            continue

        # Write dataset
        df.to_sql(name, conn)

        # Write parquet
        parq_file = here / "data" / f"{name}.csv"
        df.to_csv(parq_file, index=False)
