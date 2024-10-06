## Install Development Requirements
VegaFusion's entire development environment setup is managed by [Pixi](https://prefix.dev/docs/pixi/overview).  Pixi is a cross-platform environment manager that installs packages from [conda-forge](https://conda-forge.org/) into an isolated environment directory for the project. Conda-forge provides all the development and build technologies that are required by VegaFusion, and so Pixi is the only system installation required to build and develop VegaFusion.

### Installing Pixi
On Linux and MacOS install pixi with:

```
curl -fsSL https://pixi.sh/install.sh | bash
```

On Windows, install Pixi with:
```
iwr -useb https://pixi.sh/install.ps1 | iex
```

Then restart your shell. 

For more information on installing Pixi, see https://prefix.dev/docs/pixi/overview.

## Build and test Rust

Start the test minio server in a dedicated terminal

```
pixi run start-minio
```

Build and test the VegaFusion Rust crates with:

```
pixi run test-rs
```

Individual rust crates can be tested using:
```
pixi run test-rs-core
pixi run test-rs-runtime
pixi run test-rs-server
pixi run test-rs-sql
```

## Build and test Python

### vegafusion and vegafusion-python-embed
Build and test the `vegafusion` and `vegafusion-python-embed` Python packages with
```
pixi run test-py-vegafusion
```

### vegafusion-jupyter
Build and test the `vegafusion-jupyter` Python package with
```
pixi run test-py-jupyter-headless
```

This requires a system installation of Chrome, but the correct version of the chrome driver should be installed automatically by the `chromedriver-binary-auto` package. 

The command above will run the browser-based tests in headless mode. To instead display the browser during testing, use:

```
pixi run test-py-jupyter
```

### Try Python packages in JupyterLab
To use the development versions of the Python packages in JupyterLab, first install the packages in development mode with:

```
pixi run dev-py-jupyter
```

Then launch JupyterLab

```
pixi run jupyter-lab
```

## Build Python packages for distribution
To build Python wheels for the current platform, the `build-py-embed`, `build-py-vegafusion`, and `build-py-jupyter` tasks may be used

```
pixi run build-py-embed
```
This will build a wheel and sdist in the `target/wheels` directory.

```
pixi run build-py-vegafusion
```

This will build a wheel and sdist to the `python/vegafusion/dist` directory

```
pixi run build-py-jupyter
```

This will build a wheel and sdist to the `python/vegafusion-jupyter/dist` directory


## Making a change to development conda environment requirements
To add a conda-forge package to the Pixi development environment, use the `pixi add` command with the `--build` flag

```
pixi add my-new-package --build
```

This will install the package and update the pixi.lock file with the new environment solution.
