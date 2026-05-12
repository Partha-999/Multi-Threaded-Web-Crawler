from copy import deepcopy
from pathlib import Path

import yaml

DEFAULT_CONFIG = {
    "crawler": {
        "start_urls": [
            "https://en.wikipedia.org/wiki/Main_Page",
            "https://en.wikipedia.org/wiki/Mathematics",
            "https://en.wikipedia.org/wiki/Physics",
            "https://en.wikipedia.org/wiki/Computer_science",
            "https://en.wikipedia.org/wiki/India",
            "https://en.wikipedia.org/wiki/Artificial_intelligence",
            "https://en.wikipedia.org/wiki/World_War_II",
        ],
        "max_pages": 1000000,
        "num_threads": 32,
        "max_depth": 3,
        "retry_attempts": 3,
        "worker_batch_size": 25,
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic": "crawl-urls",
        "group_id": "crawler-workers",
        "batch_size": 25,
    },
    "elasticsearch": {
        "hosts": ["http://localhost:9200"],
        "index": "web_pages",
        "bulk_size": 500,
    },
    "redis": {
        "url": "redis://localhost:6379/0",
        "namespace": "crawler",
        "lease_seconds": 300,
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
        config["crawler"].update(
            {
                key: raw_config.get(key, value)
                for key, value in DEFAULT_CONFIG["crawler"].items()
            }
        )
    else:
        config["crawler"].update(raw_config.get("crawler", {}))

    config["kafka"].update(raw_config.get("kafka", {}))
    config["elasticsearch"].update(raw_config.get("elasticsearch", {}))
    config["redis"].update(raw_config.get("redis", {}))

    hosts = config["elasticsearch"].get("hosts")
    if isinstance(hosts, str):
        config["elasticsearch"]["hosts"] = [hosts]

    return config
