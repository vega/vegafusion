import json
from pathlib import Path


import vegafusion as vf

here = Path(__file__).parent

spec_dir = here / ".." / ".." / "vegafusion-runtime" / "tests" / "specs"


def test_get_column_usage():
    spec_file = spec_dir / "vegalite" / "concat_marginal_histograms.vg.json"
    spec = json.loads(spec_file.read_text("utf8"))
    usages = vf.get_column_usage(spec)

    assert usages == {"source_0": ["IMDB Rating", "Rotten Tomatoes Rating"]}
