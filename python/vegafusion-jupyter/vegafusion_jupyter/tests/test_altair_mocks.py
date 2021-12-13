from pathlib import Path
import jupytext
import io
import tempfile
from subprocess import Popen
import chromedriver_binary
import pytest
from selenium.webdriver import ActionChains
from selenium import webdriver
import time
from io import BytesIO
from skimage.io import imread
from skimage.metrics import structural_similarity as ssim
import json

here = Path(__file__).parent
altair_mocks_dir = here / "altair_mocks"
temp_notebooks_dir = here / "temp_notebooks"
temp_screenshots_dir = here / "temp_screenshot"

altair_markdown_template = r"""
```markdown
# Altair default
```

```python
{code}
```

```python
assert(alt.renderers.active == "default")
assert(alt.data_transformers.active == 'default')
```
"""

vegafusion_arrow_markdown_template = r"""
```markdown
# VegaFusion with Arrow data transformer
```

```python
import altair as alt
import vegafusion_jupyter
```

```python
{code}
```

```python
assert(alt.renderers.active == "vegafusion")
assert(alt.data_transformers.active == 'vegafusion-arrow')
```
"""

vegafusion_default_markdown_template = r"""

```markdown
# VegaFusion with default data transformer
```

```python
import altair as alt
import vegafusion_jupyter
alt.data_transformers.enable("default");
```

```python
{code}
```

```python
assert(alt.renderers.active == "vegafusion")
assert(alt.data_transformers.active == 'default')
```
"""


@pytest.mark.parametrize("mock_name", ["selection_layer_bar_month"])
def test_altair_mock(mock_name):

    # Build Jupytext markdown text containing the mock's code
    mock_path = altair_mocks_dir / mock_name / "mock.py"
    actions = load_actions(mock_name)

    mock_code = mock_path.read_text()
    altair_markdown = altair_markdown_template.replace("{code}", mock_code)
    vegafusion_arrow_markdown = vegafusion_arrow_markdown_template.replace("{code}", mock_code)
    vegafusion_default_markdown = vegafusion_default_markdown_template.replace("{code}", mock_code)

    # Use jupytext to convert markdown to an ipynb file
    altair_notebook = jupytext.read(io.StringIO(altair_markdown), fmt="markdown")
    vegafusion_arrow_notebook = jupytext.read(io.StringIO(vegafusion_arrow_markdown), fmt="markdown")
    vegafusion_default_notebook = jupytext.read(io.StringIO(vegafusion_default_markdown), fmt="markdown")

    # Create selenium Chrome instance
    chrome_driver = webdriver.Chrome()
    chrome_driver.set_window_size(800, 800)

    # Launch Voila server
    voila_proc = Popen(["voila", "--no-browser", "--enable_nbextensions=True"], cwd=temp_notebooks_dir)
    time.sleep(2)

    try:
        altair_imgs = export_image_sequence(chrome_driver, altair_notebook, actions)
        vegafusion_arrow_imgs = export_image_sequence(chrome_driver, vegafusion_arrow_notebook, actions)
        vegafusion_default_imgs = export_image_sequence(chrome_driver, vegafusion_default_notebook, actions)

        for i in range(len(altair_imgs)):
            altair_img = altair_imgs[i]
            vegafusion_arrow_img = vegafusion_arrow_imgs[i]
            vegafusion_default_img = vegafusion_default_imgs[i]

            similarity_arrow_value = ssim(altair_img, vegafusion_arrow_img, channel_axis=2)
            similarity_default_value = ssim(altair_img, vegafusion_default_img, channel_axis=2)

            print(f"({i}) {similarity_arrow_value=}")
            print(f"({i}) {similarity_default_value=}")

            assert similarity_arrow_value > 0.99, f"Similarity failed with Arrow data transformer on image {i}"
            assert similarity_default_value > 0.99, f"Similarity failed with default data transformer on image {i}"

    finally:
        voila_proc.kill()
        chrome_driver.close()


def load_actions(mock_name):
    actions_path = altair_mocks_dir / mock_name / "actions.json"
    if actions_path.exists():
        return json.loads(actions_path.read_text())
    else:
        return [{"type": "snapshot"}]


def export_image_sequence(
        chrome_driver: webdriver.Chrome,
        notebook: jupytext.jupytext.NotebookNode,
        actions,
        voila_url_base: str = "http://localhost:8866/voila/render/",
):
    imgs = []
    temp_notebooks_dir.mkdir(parents=True, exist_ok=True)
    temp_screenshots_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="wt", dir=temp_notebooks_dir, suffix=".ipynb") as f:
        jupytext.write(notebook, f, fmt="ipynb")
        f.file.flush()

        temp_file_path = Path(f.name)

        # Server file with voila
        url = voila_url_base.rstrip("/") + "/" + temp_file_path.name

        # Open url with selenium
        chrome_driver.get(url)

        # Get canvas element (the canvas that Vega renders to)
        time.sleep(2)
        canvas = chrome_driver.find_element_by_xpath("//canvas")

        # Process actions
        chain = ActionChains(chrome_driver)
        for i, action in enumerate(actions):
            action_type = action["type"]
            if action_type == "snapshot":
                chain.perform()
                time.sleep(2)

                # Get png representation in binary (bytes) from driver
                # convert this into a 3D numpy image
                canvas.screenshot((temp_screenshots_dir / (temp_file_path.name + ".png")).as_posix())
                img = imread(BytesIO(canvas.screenshot_as_png))
                imgs.append(img)

                # Write to file for debugging
                canvas.screenshot((temp_screenshots_dir / (temp_file_path.name + f"_{i}.png")).as_posix())

                # Reset chain
                chain = ActionChains(chrome_driver)

            elif action_type == "move_to":
                coords = action["coords"]
                chain = chain.move_to_element_with_offset(canvas, coords[0], coords[1])
            elif action_type == "move_by":
                coords = action["coords"]
                chain = chain.move_by_offset(coords[0], coords[1])
            elif action_type == "click_and_hold":
                chain = chain.click_and_hold()
            elif action_type == "click":
                chain = chain.click()
            elif action_type == "double_click":
                chain = chain.double_click()
            elif action_type == "release":
                chain = chain.release()
            else:
                raise ValueError(f"Invalid action type {action_type}")

    return imgs
