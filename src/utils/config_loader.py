from pathlib import Path
from box import Box
import json


def load_config() -> Box:
    """Load config JSON into a Box object for dot-notation access."""
    config_path = Path(__file__).resolve().parents[2] / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return Box(data, frozen_box=True)

config = load_config()
