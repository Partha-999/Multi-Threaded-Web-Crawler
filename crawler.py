import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import psutil
import requests
from bs4 import BeautifulSoup

from distributed_pipeline import DistributedSearchPipeline
from frontier import URLFrontier
from settings import load_config

config = load_config()
start_urls = config["crawler"]["start_urls"]
max_pages = int(config["crawler"]["max_pages"])
max_depth = int(config["crawler"]["max_depth"])
retry_attempts = int(config["crawler"]["retry_attempts"])
worker_batch_size = int(config["crawler"].get("worker_batch_size", 25))
num_threads = int(config["crawler"].get("num_threads", 20))
lease_seconds = int(config["redis"].get("lease_seconds", 300))

VALID_DOMAINS = ["wikipedia.org", "arxiv.org", ".edu"]
JUNK_PATTERNS = [
    "login",
    "signin",
    "signup",
    "register",
    "special:",
    "user:",
    "user_talk:",
    "talk:",
    "edit",
    "history",
    "file:",
    "category:",
]


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key in (
            "worker_id",
            "url",
            "depth",
            "queue_size",
            "inflight_size",
            "indexed_rate",
            "batch_size",
            "pages_crawled",
        ):
            value = record.__dict__.get(key)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
logger.handlers = [handler]
logger.propagate = False

frontier = URLFrontier(
    redis_url=config["redis"]["url"],
    namespace=config["redis"]["namespace"],
)
pipeline = DistributedSearchPipeline(config)
_thread_local = threading.local()


def get_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; MiniSearchBot/1.0; +https://example.com/bot)",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        _thread_local.session = session
    return session


def clean_url(url):
    return url.split("#")[0].rstrip("/")


def is_valid_url(url):
    if not url:
        return False

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False

    netloc = parsed.netloc.lower()
    if not netloc:
        return False

    return any(domain in netloc for domain in VALID_DOMAINS)


def is_junk_url(url):
    lowered = url.lower()
    return any(pattern in lowered for pattern in JUNK_PATTERNS)


def fetch_page(url, depth):
    session = get_session()
    for attempt in range(retry_attempts):
        try:
            response = session.get(url, timeout=(5, 20))
            response.raise_for_status()
            return response.text
        except requests.RequestException:
            wait_time = 2**attempt
            logger.warning(
                "fetch_retry",
                extra={
                    "url": url,
                    "depth": depth,
                    "queue_size": frontier.queue_size(),
                    "batch_size": attempt + 1,
                },
            )
            time.sleep(wait_time)
    return None


def parse_page(content, url):
    soup = BeautifulSoup(content, "html.parser")
    title = (
        soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    )
    text = soup.get_text(" ", strip=True)
    links = []

    for anchor in soup.find_all("a", href=True):
        link = clean_url(urljoin(url, anchor["href"]))
        if not is_valid_url(link) or is_junk_url(link):
            continue
        links.append(link)

    return title, text, list(dict.fromkeys(links))


def crawl_item(item, worker_id):
    payload = item["_payload"]
    url = item["url"]
    depth = int(item.get("depth", 0))

    if depth > max_depth:
        frontier.acknowledge(payload, url)
        return 0, 0

    logger.info(
        "crawl_url",
        extra={
            "worker_id": worker_id,
            "url": url,
            "depth": depth,
            "queue_size": frontier.queue_size(),
            "inflight_size": frontier.inflight_size(),
            "indexed_rate": 0,
        },
    )

    html = fetch_page(url, depth)
    if not html:
        frontier.acknowledge(payload, url)
        return 0, 0

    title, content, links = parse_page(html, url)
    pipeline.index_page(url, title, content, links)

    next_items = [{"url": link, "depth": depth + 1} for link in links]
    if next_items:
        frontier.enqueue_many(next_items)
        pipeline.publish_urls(next_items)

    frontier.acknowledge(payload, url)
    return len(content), len(links)


def monitor_system():
    while True:
        logger.info(
            "system_usage",
            extra={
                "queue_size": frontier.queue_size(),
                "inflight_size": frontier.inflight_size(),
                "pages_crawled": frontier.visited_count(),
                "indexed_rate": 0,
            },
        )
        psutil.cpu_percent(interval=1)
        time.sleep(4)


def seed_frontier():
    if not frontier.is_empty():
        return

    seed_items = [
        {"url": clean_url(url), "depth": 0} for url in start_urls if is_valid_url(url)
    ]
    if seed_items:
        frontier.enqueue_many(seed_items)
        pipeline.publish_urls(seed_items)
        logger.info(
            "seeded_frontier",
            extra={
                "queue_size": frontier.queue_size(),
                "inflight_size": frontier.inflight_size(),
                "pages_crawled": frontier.visited_count(),
                "batch_size": len(seed_items),
            },
        )


def crawl_worker(worker_id):
    processed = 0
    started_at = time.time()

    while frontier.visited_count() < max_pages:
        frontier.requeue_stale(lease_seconds)
        batch = frontier.reserve_batch(
            worker_id, batch_size=worker_batch_size, lease_seconds=lease_seconds
        )

        if not batch:
            if frontier.is_empty():
                break
            time.sleep(1)
            continue

        with ThreadPoolExecutor(max_workers=min(num_threads, len(batch))) as executor:
            futures = {
                executor.submit(crawl_item, item, worker_id): item for item in batch
            }
            batch_processed = 0

            for future in as_completed(futures):
                future.result()
                batch_processed += 1

        processed += batch_processed
        pipeline.flush_bulk()
        pipeline.flush_urls()

        elapsed = max(time.time() - started_at, 1)
        rate = round(processed / elapsed, 2)

        logger.info(
            "crawl_batch_complete",
            extra={
                "worker_id": worker_id,
                "batch_size": len(batch),
                "queue_size": frontier.queue_size(),
                "inflight_size": frontier.inflight_size(),
                "pages_crawled": frontier.visited_count(),
                "indexed_rate": rate,
            },
        )

        if frontier.visited_count() >= max_pages:
            break


def main():
    seed_frontier()

    monitor_thread = threading.Thread(target=monitor_system, daemon=True)
    monitor_thread.start()

    worker_id = f"worker-{os.getpid()}-{threading.get_ident()}"
    crawl_worker(worker_id)

    pipeline.close()
    frontier.close()


if __name__ == "__main__":
    main()
