"""Download datafusion proto files matching the workspace dependency version.

Downloads the main datafusion.proto, then recursively downloads any
proto files it imports.
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CARGO_TOML = ROOT / "Cargo.toml"
PROTO_DIR = ROOT / "vegafusion-python" / "proto"

# The main proto file to download (path within the datafusion repo)
MAIN_PROTO = "datafusion/proto/proto/datafusion.proto"

BASE_URL = "https://raw.githubusercontent.com/apache/datafusion/"


def get_datafusion_version() -> str:
    text = CARGO_TOML.read_text()
    match = re.search(
        r"\[workspace\.dependencies\.datafusion\]\s*\n\s*version\s*=\s*\"=?([^\"]+)\"",
        text,
    )
    if not match:
        print("ERROR: Could not find datafusion version in Cargo.toml", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def download_file(url: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["curl", "-sfL", url, "-o", str(output)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: curl failed for {url} (exit {result.returncode})", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)

    # Sanity check
    content = output.read_text()
    if "syntax" not in content[:5000]:
        print(f"ERROR: {output} doesn't look like a .proto file", file=sys.stderr)
        print(f"  First 200 chars: {content[:200]}", file=sys.stderr)
        sys.exit(1)


def find_imports(proto_path: Path) -> list[str]:
    """Extract import paths from a .proto file."""
    return re.findall(r'^import\s+"([^"]+)"\s*;', proto_path.read_text(), re.MULTILINE)


def main():
    version = get_datafusion_version()
    print(f"Downloading datafusion proto files for version {version}")

    # Download main proto file into PROTO_DIR root
    main_url = f"{BASE_URL}{version}/{MAIN_PROTO}"
    main_output = PROTO_DIR / "datafusion.proto"
    print(f"  datafusion.proto")
    download_file(main_url, main_output)

    # Recursively download imported proto files
    downloaded: set[str] = set()
    to_process = [main_output]

    while to_process:
        proto_file = to_process.pop()
        for import_path in find_imports(proto_file):
            if import_path in downloaded:
                continue
            downloaded.add(import_path)

            url = f"{BASE_URL}{version}/{import_path}"
            output = PROTO_DIR / import_path
            print(f"  {import_path}")
            download_file(url, output)
            to_process.append(output)

    # Write source info for traceability
    all_files = ["datafusion.proto"] + sorted(downloaded)
    source_info = PROTO_DIR / "SOURCE.txt"
    source_info.write_text(
        f"Downloaded from: {BASE_URL}{version}/\n"
        f"DataFusion version: {version}\n"
        f"Files:\n" + "".join(f"  {f}\n" for f in all_files)
    )

    print("Done.")


if __name__ == "__main__":
    main()
