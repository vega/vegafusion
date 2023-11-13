from minio import Minio
from minio.error import S3Error
import os
import subprocess
import time
from pathlib import Path
import shutil
import pandas as pd
from csv import QUOTE_ALL
from io import BytesIO
import pyarrow as pa

root = Path(__file__).parent.parent


def main():
    access_key = "access_key123"
    secret_key = "secret_key123"
    print("Starting minio")
    p = start_minio_server(access_key, secret_key)
    time.sleep(1)

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        "localhost:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    # Make 'data' bucket if it does not exist
    print("Loading test data to the 'data' bucket")
    found = client.bucket_exists("data")
    if not found:
        client.make_bucket("data")
    else:
        print("Bucket 'data' already exists")

    # Put original json object
    movies_json_path = root / "vegafusion-runtime" / "tests" / "util" / "vegajs_runtime" / "data" / "movies.json"
    client.fput_object(
        "data",
        "movies.json",
        movies_json_path,
    )

    # load as pandas
    df = pd.read_json(movies_json_path)
    df["Title"] = df.Title.astype(str)
    df["Release Date"] = pd.to_datetime(df["Release Date"])

    # Convert to csv
    f = BytesIO()
    df.to_csv(f, index=False, quoting=QUOTE_ALL)
    b = f.getvalue()
    n = len(b)

    client.put_object(
        "data",
        "movies.csv",
        BytesIO(b),
        n
    )

    # Convert to arrow
    tbl = pa.Table.from_pandas(df)
    b = arrow_table_to_ipc_bytes(tbl)
    client.put_object(
        "data",
        "movies.arrow",
        BytesIO(b),
        len(b)
    )

    # Convert to parquet. For some reason, uploading to minio with client.fput_object
    # (as above for arrow) results in a parquet file with corrupt footer.
    f = BytesIO()
    df.to_parquet(f)
    b = f.getvalue()
    n = len(b)

    client.put_object(
        "data",
        "movies.parquet",
        BytesIO(b),
        n
    )

    print("Data loaded")
    print(f"""
Open dashboard at http://127.0.0.1:9000
username: {access_key}
password: {secret_key}
""")
    # Block on the server
    # p.wait()
    p.terminate()


def start_minio_server(access_key, secret_key):
    # Set environment variables for access and secret keys
    env = os.environ.copy()
    env["MINIO_ROOT_USER"] = access_key
    env["MINIO_ROOT_PASSWORD"] = secret_key
    env["MINIO_REGION"] = "us-east-1"

    # Command to start MinIO server
    data_dir = root / "minio_data"
    shutil.rmtree(data_dir, ignore_errors=True)
    cmd = ["minio", "server", "minio_data"]

    # Start MinIO server in the background
    process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return process


def arrow_table_to_ipc_bytes(table):
    bytes_buffer = BytesIO()
    max_chunksize=8096
    with pa.ipc.new_file(bytes_buffer, table.schema) as f:
        f.write_table(table, max_chunksize=max_chunksize)

    return bytes_buffer.getvalue()


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)