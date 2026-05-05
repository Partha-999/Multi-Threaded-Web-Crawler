from copy import deepcopy
from pathlib import Path

import yaml


DEFAULT_CONFIG = {
    "crawler": {
        "start_url": "https://en.wikipedia.org/wiki/Ram_Charan",
        "max_pages": 50,
        "num_threads": 10,
        "max_depth": 3,
        "retry_attempts": 3,
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic": "crawl-urls",
        "group_id": "crawler-workers",
    },
    "elasticsearch": {
        "hosts": ["http://localhost:9200"],
        "index": "web_pages",
    },
}


def load_config(path="config.yaml"):
    config = deepcopy(DEFAULT_CONFIG)
    config_path = Path(path)

    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as file:
            raw_config = yaml.safe_load(file) or {}
    else:
        raw_config = {}

    if any(key in raw_config for key in DEFAULT_CONFIG["crawler"]):
        config["crawler"].update({key: raw_config.get(key, value) for key, value in DEFAULT_CONFIG["crawler"].items()})
    else:
        config["crawler"].update(raw_config.get("crawler", {}))

    config["kafka"].update(raw_config.get("kafka", {}))
    config["elasticsearch"].update(raw_config.get("elasticsearch", {}))

    return config