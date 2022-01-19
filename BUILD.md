## Install Development Requirements
To get started developing VegaFusion, several technologies need to be installed 

### Install Rust
Install Rust, following the instructions at https://www.rust-lang.org/tools/install

### Install miniconda
Install miniconda, following instructions at https://docs.conda.io/en/latest/miniconda.html.

If you already have conda installed, you can skip this step.

### Install wasm-pack
Install `wasm-pack`, following instructions at https://rustwasm.github.io/wasm-pack/installer/

### Create conda development environment

For Linux:
```bash
conda env create --name vegafusion_dev --file python/vegafusion-jupyter/conda-linux-64-3.10.lock
```

For MacOS:
```bash
conda env create --name vegafusion_dev --file python/vegafusion-jupyter/conda-osx-64-3.10.lock
```

For Windows:
```bash
conda env create --name vegafusion_dev --file python/vegafusion-jupyter/conda-win-64-3.10.lock
```

### Activate conda development environment
```bash
conda activate vegafusion_dev
```

### Install test npm dependencies
Note: The `npm` command is included in the `nodejs` conda-forge package installed in the development conda environment.

```bash
From the repository root:
cd vegafusion-rt-datafusion/tests/util/vegajs_runtime/
npm install
```

### Install Chrome
Install desktop Chrome with version matching the `python-chromedriver-binary` version from the conda lock file above. e.g. `96.0`.  Or, install the version of `python-chromedriver-binary` that matches the version of chrome you already have installed. 

## Install/Build packages
VegaFusion contains several packages using a variety of languages and build tools that need to be built before running the test suite.

### Build `vegafusion-wasm` package
From the repository root:
```bash
cd vegafusion-wasm
wasm-pack build --release
```

### Link `vegafusion-wasm`
```bash
cd vegafusion-wasm/pkg
npm link
```

### Build the `vegafusion-python` PyO3 Python package in development mode
Note: The PyO3 maturin build tool was included in the `maturin` conda-forge package installed in the development conda environment.

From the repository root:
```bash
cd vegafusion-python
maturin develop --release
```

### Install the `vegafusion-jupyter` Python package in development mode

From the repository root:
```bash
cd python/vegafusion-jupyter/
cd npm link vegafusion-wasm
pip install -e ".[test]"
```

Then, build the jupyterlab extension

```bash
jupyter labextension develop --overwrite .
yarn run build
```

## Running tests

### Run the Rust tests
From the repository root:
```bash
cargo test
```

### Run the vegafusion-jupyter integration tests
From the repository root:
```bash
cd python/vegafusion-jupyter/
pytest tests
```

To run the tests in headless mode (so that the chrome browser window doesn't pop up), set the `VEGAFUSION_TEST_HEADLESS` environment variable.

```bash
VEGAFUSION_TEST_HEADLESS=1 pytest tests
```

## Build Python packages for distribution
The instructions above build the python packages for development. To build standalone python wheels, use `maturin build` instead of `maturin develop`.  Also, include the flags below to support manylinux 2010 standard on Linux and universal2 on MacOS.

From repository root:
```bash
cd vegafusion-python
maturin build --release --strip --manylinux 2010
```

This will create wheel files for `vegafusion-python` in the `target/wheels` directory.

From repository root:
```bash
cd python/vegafusion-jupyter/
python setup.py bdist_wheel
```

### (MacOs) Cross compile to Apple Silicon from Intel
In order to compile for the Apple Silicon (M1) architecture from an intel Mac, the `aarch64-apple-darwin` Rust toolchain.

```bash
rustup target add aarch64-apple-darwin
```

Then build MacOS Apple Silicon package with 
```bash
maturin build --release --strip --target aarch64-apple-darwin
```

## Making a change to development conda environment requirements

1. Edit the dev-environment-3.X.yml file
2. Update lock files with
```bash
cd python/vegafusion-jupyter/
conda-lock -f dev-environment-3.7.yml -p osx-64 -p linux-64 -p win-64 --filename-template "conda-{platform}-cp37.lock"
conda-lock -f dev-environment-3.10.yml -p osx-64 -p linux-64 -p win-64 --filename-template "conda-{platform}-cp310.lock"
```
