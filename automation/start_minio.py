from minio import Minio
from minio.error import S3Error
import os
import subprocess
import time
from pathlib import Path
import shutil

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

    for fname in ["movies.json", "barley.json", "disasters.csv", "github.csv"]:
        client.fput_object(
            "data",
            fname, root / "vegafusion-runtime" / "tests" / "util" / "vegajs_runtime" / "data" / fname,
        )

    print("Data loaded")
    print(f"""
Open dashboard at http://127.0.0.1:9000
username: {access_key}
password: {secret_key}
""")
    # Block on the server
    p.wait()


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


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)