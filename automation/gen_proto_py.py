"""Generate Python protobuf code from downloaded .proto files.

Handles the split proto structure (datafusion.proto + datafusion_common.proto)
and fixes import paths so they work within the vegafusion.proto package.
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "vegafusion-python" / "proto"
OUTPUT_DIR = ROOT / "vegafusion-python" / "vegafusion" / "proto"


def find_proto_files() -> list[Path]:
    """Find all .proto files under the proto directory."""
    return sorted(PROTO_DIR.rglob("*.proto"))


def run_protoc(proto_files: list[Path]) -> None:
    """Run protoc to generate Python code."""
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUTPUT_DIR}",
    ] + [str(f) for f in proto_files]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: protoc failed", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)


def fix_imports(pb2_file: Path) -> None:
    """Fix absolute imports to relative imports in generated pb2 files.

    protoc generates absolute imports like:
        from datafusion.proto_common.proto import datafusion_common_pb2
    which conflicts with the installed `datafusion` PyPI package.
    We convert these to relative imports.
    """
    content = pb2_file.read_text()
    original = content

    # Fix: from datafusion.X import Y -> from .datafusion.X import Y
    # Only fix imports that reference our generated proto subpackages
    content = re.sub(
        r"^(from )(datafusion\.)(.+? import .+)$",
        r"\1.\2\3",
        content,
        flags=re.MULTILINE,
    )

    if content != original:
        pb2_file.write_text(content)
        print(f"  Fixed imports in {pb2_file.relative_to(ROOT)}")


def ensure_init_files() -> None:
    """Create __init__.py files for any new proto subpackages."""
    for dirpath in OUTPUT_DIR.rglob("*"):
        if dirpath.is_dir() and dirpath != OUTPUT_DIR and "__pycache__" not in str(dirpath):
            init = dirpath / "__init__.py"
            if not init.exists():
                init.write_text("")
                print(f"  Created {init.relative_to(ROOT)}")


def main():
    proto_files = find_proto_files()
    if not proto_files:
        print("ERROR: No .proto files found", file=sys.stderr)
        sys.exit(1)

    print(f"Generating Python protobuf code from {len(proto_files)} proto file(s)")
    for f in proto_files:
        print(f"  {f.relative_to(PROTO_DIR)}")

    run_protoc(proto_files)
    ensure_init_files()

    # Fix imports in all generated pb2 files
    for pb2 in OUTPUT_DIR.rglob("*_pb2.py"):
        fix_imports(pb2)

    # Format generated files
    pb2_files = list(OUTPUT_DIR.rglob("*_pb2.py"))
    if pb2_files:
        result = subprocess.run(
            ["ruff", "format"] + [str(f) for f in pb2_files],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("WARNING: ruff format failed", file=sys.stderr)
            if result.stderr:
                print(result.stderr, file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()
