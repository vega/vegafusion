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
conda create --name vegafusion_dev --file python/vegafusion-jupyter/conda-linux-64-cp310.lock
```

For MacOS (Intel):
```bash
conda create --name vegafusion_dev --file python/vegafusion-jupyter/conda-osx-64-cp310.lock
```

For MacOS (Arm):
```bash
conda create --name vegafusion_dev --file python/vegafusion-jupyter/conda-osx-arm64-cp310.lock
```

For Windows:
```bash
conda create --name vegafusion_dev --file python/vegafusion-jupyter/conda-win-64-cp310.lock
```

### Activate conda development environment
```bash
conda activate vegafusion_dev
```

### Install Chrome and Chrome Webdriver
Install Chrome desktop and a compatible Chrome Driver:

For MacOS
```
$ brew install --cask chromedriver
```

For Linux and Windows, this can be installed with conda
```
$ conda install -c conda-forge python-chromedriver-binary
```

In both cases, make sure the chromedriver version is compatible with the version of Chrome desktop

### Install test npm dependencies
Note: The `npm` command is included in the `nodejs` conda-forge package installed in the development conda environment.

From the repository root:
```bash
cd vegafusion-runtime/tests/util/vegajs_runtime/
npm install
```

Note: On a MacOS ARM machine, npm may attempt to build the node canvas dependency from source. The following dependencies may be needed.
```
$ brew install pkg-config cairo pango libpng jpeg giflib librsvg
```
See https://stackoverflow.com/questions/71352368/is-it-possible-to-install-canvas-with-m1-chip

## Install/Build packages
VegaFusion contains several packages using a variety of languages and build tools that need to be built before running the test suite.

### Build `vegafusion-wasm` package
From the repository root:
```bash
cd vegafusion-wasm
npm install
npm run build
```

### Build `javascript/vegafusion-embed` package
From the repository root:
```bash
cd javascript/vegafusion-embed
npm install
npm run build
```

### Build the `vegafusion-python` PyO3 Python package in development mode
Note: The PyO3 maturin build tool was included in the `maturin` conda-forge package installed in the development conda environment.

From the repository root:
```bash
cd vegafusion-python-embed
maturin develop --release
```

### Install the `vegafusion` Python package in development mode
From the repository root:
```bash
cd python/vegafusion/
pip install -e .
```

### Install the `vegafusion-jupyter` Python package in development mode
From the repository root:
```bash
cd python/vegafusion-jupyter/
npm install
pip install -e ".[test]"
```

Then, build the jupyterlab extension

```bash
npm run build:dev
```

## Running tests

### Run the Rust tests
From the repository root:
```bash
cargo test --workspace --exclude vegafusion-python-embed --exclude vegafusion-wasm
```

### Run the vegafusion Python tests
From the repository root:
```bash
cd python/vegafusion/
pytest tests
```

### Run the vegafusion-jupyter Python integration tests
From the repository root:
```bash
cd python/vegafusion-jupyter/
pytest tests
```

To run the tests in headless mode (so that the chrome browser window doesn't pop up), set the `VEGAFUSION_TEST_HEADLESS` environment variable.

```bash
VEGAFUSION_TEST_HEADLESS=1 pytest tests
```

### Java tests
Follow instructions in [java/README.md](java/README.md).

## Build Python packages for distribution
The instructions above build the python packages for development. To build standalone python wheels, use `maturin build` instead of `maturin develop`.  Also, include the flags below to support manylinux 2010 standard on Linux and universal2 on MacOS.

From repository root:
```bash
cd vegafusion-python-embed
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

Note: the 3.7 environment is not supported on `osx-arm64`
```bash
cd python/vegafusion-jupyter/
conda-lock --no-mamba -f dev-environment-3.8.yml --kind explicit -p osx-64 -p linux-64 -p win-64 --filename-template "conda-{platform}-cp38.lock"
conda-lock --no-mamba -f dev-environment-3.10.yml --kind explicit -p osx-64 -p osx-arm64 -p linux-64 -p win-64 --filename-template "conda-{platform}-cp310.lock"
```

Note: the `--no-mamba` flag is due to https://github.com/conda/conda-lock/issues/381.
