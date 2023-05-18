import click
from pathlib import Path
import toml
import json
import configparser

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
        "vegafusion-datafusion-udfs",
        "vegafusion-dataframe",
        "vegafusion-sql",
        "vegafusion-runtime",
        "vegafusion-python-embed",
        "vegafusion-server",
        "vegafusion-wasm",
        "vegafusion-jni",
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
    package_json_dirs = [
        root / "vegafusion-wasm",
        root / "javascript" / "vegafusion-embed",
        root / "python" / "vegafusion-jupyter"
    ]
    for package_json_dir in package_json_dirs:
        for fname in ["package.json", "package-lock.json"]:
            package_json_path = package_json_dir / fname
            package_json = json.loads(package_json_path.read_text())
            package_json["version"] = version
            package_json_path.write_text(json.dumps(package_json, indent=2))
            print(f"Updated version in {package_json_path}")

    # Handle _version.py files
    version_py_dirs = [
        root / "python" / "vegafusion" / "vegafusion",
        root / "python" / "vegafusion-jupyter" / "vegafusion_jupyter",
        ]
    for version_py_dir in version_py_dirs:
        version_py_path = version_py_dir / "_version.py"
        version_py_path.write_text(f'__version__ = {repr(version)}\n')
        print(f"Updated version in {version_py_path}")

    # Handle cfg files
    cfg_dirs = [
        root / "python" / "vegafusion"
    ]
    for cfg_dir in cfg_dirs:
        cgf_path = cfg_dir / "setup.cfg"
        parser = configparser.ConfigParser()
        parser.read_string(cgf_path.read_text())

        # Set package version
        parser.set("metadata", "version", version)

        # Check for embed dependencies
        if parser.has_option("options.extras_require", "embed"):
            deps = parser.get("options.extras_require", "embed").split("\n")
            new_deps = []
            for dep in deps:
                if dep.strip().startswith("vegafusion-python-embed"):
                    new_deps.append(f"vegafusion-python-embed=={version}")
                elif dep.strip().startswith("vegafusion"):
                    new_deps.append(f"vegafusion=={version}")
                else:
                    new_deps.append(dep)

            deps_str = "\n".join(new_deps)
            parser.set("options.extras_require", "embed", deps_str)

        with cgf_path.open("wt") as f:
            parser.write(f)
        print(f"Updated version in {cgf_path}")

    # Handle _frontend.py
    frontend_py_path = root / "python" / "vegafusion-jupyter" / "vegafusion_jupyter" / "_frontend.py"
    frontend_py_path.write_text(f"""\
\"\"\"
Information about the frontend package of the widgets.
\"\"\"    
module_name = "vegafusion-jupyter"
module_version = "^{version}"
""")
    print(f"Updated version in {frontend_py_path}")

    # Handle java/version.txt
    version_txt_path = root / "java" / "version.txt"
    with open(version_txt_path, "wt") as f:
        f.write(version)
    print(f"Updated version in {version_txt_path}")


if __name__ == '__main__':
    bump_version()
