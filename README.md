<img src="https://user-images.githubusercontent.com/15064365/213880031-eca5ab61-3d97-474f-a0e6-3662dbc9887f.svg#gh-dark-mode-only" width=50%>

<img src="https://user-images.githubusercontent.com/15064365/213880036-3d28c1b6-5b76-47c4-a010-2a623522c9f2.svg#gh-light-mode-only" width=50%>

---
# Overview
VegaFusion provides Rust, Python, and JavaScript libraries for analyzing and scaling [Vega](https://vega.github.io/vega/) visualizations. The goal is to provide low-level building blocks that higher level Vega systems (such as [Vega-Altair](https://altair-viz.github.io/) in Python) can integrate with.

> [!NOTE]  
> If you've arrived here looking for information on how to scale Vega-Altair visualizations to support larger datasets, 
> see the Vega-Altair documentation on the [`"vegafusion"` data transformer](https://altair-viz.github.io/user_guide/large_datasets.html#vegafusion-data-transformer).

## Python Installation

The VegaFusion Python package can be installed into a Python environment using pip

```bash
pip install vegafusion
```

or conda

```bash
conda install -c conda-forge vegafusion
```

## Documentation
See [Documentation](https://vegafusion.io/) and [Examples](https://github.com/vega/vegafusion/tree/v2/examples/python-examples/chart_state.py).

## Development

### Prerequisites

VegaFusion development requires:
- **Rust**: Install from [rustup.rs](https://rustup.rs/)
- **Pixi**: Install from [pixi.sh](https://pixi.sh/) - manages all other dependencies

### Setup

```bash
# Clone the repository
git clone https://github.com/vega/vegafusion.git
cd vegafusion

# Install Pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | bash

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Run any pixi task (this will automatically install dependencies)
pixi run test-rs
```

See [CLAUDE.md](CLAUDE.md) for detailed development instructions.