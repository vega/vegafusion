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
To publish the `vegafusion-python-embed` packages to PyPI, first download and unzip the `vegafusion-python-embed-wheels` artifacts from GitHub Actions. Then `cd` into the directory of `*.whl` and `*.tar.gz` files and upload the packages to PyPI using [twine](https://pypi.org/project/twine/).

```bash
cd vegafusion-python-embed-wheels
twine upload *
```

To publish the `vegafusion` packages, download and unzip the `vegafusion-wheel` artifacts. Then upload with twine.

```bash
cd vegafusion-packages
twine upload *
```

To publish the `vegafusion-jupyter` packages, download and unzip the `vegafusion-jupyter-packages` artifacts. Then upload with twine.

```bash
cd vegafusion-jupyter-packages
twine upload *
```

### Publish NPM packages
First, download and unzip the `vegafusion-wasm-packages` artifact. Then publish the `vegafusion-wasm-X.Y.Z.tgz` package to NPM.  If this is a release candidate, include the `--pre` flag to `npm publish`.

```bash
unzip vegafusion-wasm-packages.zip
npm publish vegafusion-wasm-$VF_VERSION.tgz
```

Next, change the version of `vegafusion-wasm` in `javascript/vegafusion-embed/package.json` from `"../../vegafusion-wasm/pkg"` to `"~X.Y.Z"`

Then update `package.lock`, and build package, then publish to NPM (include the `--pre` flag to `npm publish` if this is a release candidate)
```bash
cd javascript/vegafusion-embed/
npm install
npm run build 
npm publish
```

Next, change the version of `vegafusion-wasm` and `vegafusion-embed` in `python/vegafusion-jupyter/package.json` from local paths to `"~X.Y.Z"`

Then build and publish the packages (include the `--pre` flag if this is a release candidate)

```bash
cd python/vegafusion-jupyter/
npm install
npm run build:prod
npm publish
```

### Publish Rust crates
The Rust crates should be published in the following order

```
pixi run publish-rs
```
