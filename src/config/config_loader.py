from pathlib import Path
from box import Box
import json


def load_config() -> Box:
    """Load configuration JSON into a Box object for convenient dot-notation access."""
    # config.json is located in the same directory (src/config/)
    config_path = Path(__file__).resolve().parent / "config.json"
    with open(config_path, "r", encoding="utf-8") as file_handle:
        data = json.load(file_handle)

    return Box(data, frozen_box=True)


config = load_config()