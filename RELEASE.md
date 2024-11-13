## Release process

### Release branch
To release version `X.Y.Z`, first create a and checkout a branch named `release_vX.Y.Z`

```bash
export VF_VERSION=X.Y.Z
git checkout main
git fetch
git reset --hard origin/main
git switch -c release_$VF_VERSION
```

Then update all the package version strings to `X.Y.Z` using the `automation/bump_version.py` script

```bash
python automation/bump_version.py $VF_VERSION
```

Open a pull request to merge this branch into `main`.  This will start the continuous integration jobs on GitHub Actions. These jobs will test VegaFusion and build packages for publication.

### Publish Python packages
To publish the `vegafusion` packages to PyPI, first download and unzip the `vegafusion-python-wheels-all` artifacts from GitHub Actions. Then `cd` into the directory of `*.whl` and `*.tar.gz` files and upload the packages to PyPI using [twine](https://pypi.org/project/twine/).

```bash
cd vegafusion-python-wheels-all
twine upload *
```

### Publish NPM packages
First, download and unzip the `vegafusion-wasm-packages` artifact. Then publish the `vegafusion-wasm-X.Y.Z.tgz` package to NPM.  If this is a release candidate, include the `--pre` flag to `npm publish`.

```bash
cd vegafusion-wasm-package
npm publish vegafusion-wasm-$VF_VERSION.tgz
```

### Publish Rust crates
The Rust crates should be published in the following order

```
pixi run publish-rs
```

### Create GitHub Release
Create a [GitHub Release](https://github.com/vega/vegafusion/releases) with tag `vX.Y.Z` and title `Release X.Y.Z`. Attach:
 1. the wheels from the `vegafusion-python-wheels-all` artifact
 2. The server binary archives from the `vegafusion-server-all` artifact
 2. the `vegafusion-wasm-X.Y.Z.tgz` package from the `vegafusion-wasm-package` artifact

### Publish documentation
Publish the documentation to the `gh-pages` branch of the https://github.com/vegafusion/vegafusion.github.io repository by running `pixi run docs-publish`, then confirm the push by entering `y` at the prompt.
