import click
from pathlib import Path
import toml
import json
import configparser
import subprocess

root = Path(__file__).parent.parent.absolute()


@click.command()
@click.argument('version')
def bump_version(version):
    """Simple program that greets NAME for a total of COUNT times."""
    print(f"Updating version to {version}")

    # Handle Cargo.toml files
    cargo_packages = [
        "vegafusion-common",
        "vegafusion-core",
        "vegafusion-runtime",
        "vegafusion-python",
        "vegafusion-server",
        "vegafusion-wasm",
    ]

    for package in cargo_packages:
        cargo_toml_path = (root / package / "Cargo.toml")
        cargo_toml = toml.loads(cargo_toml_path.read_text())
        # Update this package's version
        cargo_toml["package"]["version"] = version

        # Look for local workspace dependencies and update their versions
        for dep_type in ["dependencies", "dev-dependencies", "build-dependencies"]:
            for p, props in cargo_toml.get(dep_type, {}).items():
                if isinstance(props, dict) and props.get("path", "").startswith("../vegafusion"):
                    props["version"] = version

        # Fix quotes in target so that they don't get double escaped when written back out
        new_target = {}
        for target, val in cargo_toml.get("target", {}).items():
            unescaped_target = target.replace(r'\"', '"')
            new_target[unescaped_target] = val
        if new_target:
            cargo_toml["target"] = new_target

        cargo_toml_path.write_text(toml.dumps(cargo_toml))
        print(f"Updated version in {cargo_toml_path}")

    # Handle package.json files
    package_json_dirs = [root / "vegafusion-wasm"]
    for package_json_dir in package_json_dirs:
        for fname in ["package.json", "package-lock.json"]:
            package_json_path = package_json_dir / fname
            package_json = json.loads(package_json_path.read_text())
            package_json["version"] = version
            package_json_path.write_text(json.dumps(package_json, indent=2))
            print(f"Updated version in {package_json_path}")


    # Handle pyproject.toml files
    pyproject_toml_dirs = [
        root / "vegafusion-python",
    ]
    for pyproject_toml_dir in pyproject_toml_dirs:
        for fname in ["pyproject.toml"]:
            pyproject_toml_path = pyproject_toml_dir / fname
            pyproject_toml = toml.loads(pyproject_toml_path.read_text())
            pyproject_toml["project"]["version"] = version
            pyproject_toml_path.write_text(toml.dumps(pyproject_toml))
            print(f"Updated version in {pyproject_toml_path}")

    # Run taplo fmt *.toml
    print("Formatting TOML files...")
    try:
        subprocess.run(["taplo", "fmt", "vegafusion-*/**/*.toml"], cwd=root, check=True)
        print("TOML files formatted successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error formatting TOML files: {e}")
    except FileNotFoundError:
        print("taplo command not found. Make sure it's installed and in your PATH.")


if __name__ == '__main__':
    bump_version()
