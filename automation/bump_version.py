import click
from pathlib import Path
import toml

root = Path(__file__).parent.parent.absolute()

@click.command()
@click.argument('version')
def bump_version(version):
    """Simple program that greets NAME for a total of COUNT times."""
    print(f"Updating version to {version}")

    # vegafusion-core
    cargo_toml_path = (root / "vegafusion-core" / "Cargo.toml")
    cargo_toml = toml.loads(cargo_toml_path.read_text())
    cargo_toml["package"]["version"] = version
    cargo_toml_path.write_text(toml.dumps(cargo_toml))


if __name__ == '__main__':
    bump_version()