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
from urllib import robotparser

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

# politeness / domain defaults
max_frontier_queue_size = int(config["crawler"].get("max_frontier_queue_size", 200000))
priority_enabled = bool(config["crawler"].get("priority_enabled", True))
domain_max_requests_default = int(
    config["crawler"].get("domain_max_requests_default", 1000)
)
per_domain_delay_seconds = float(config["crawler"].get("per_domain_delay_seconds", 2))
robots_ttl_seconds = int(config["crawler"].get("robots_ttl_seconds", 3600))
domain_budget_map = config.get("domains", {}).get("budgets", {}) or {}

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
    config={
        "lease_seconds": lease_seconds,
        "priority_enabled": priority_enabled,
        "max_frontier_queue_size": max_frontier_queue_size,
        "domain_max_requests_default": domain_max_requests_default,
    },
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


# Pre-enqueue filtering constants
MAX_URL_LENGTH = 200
BLOCKED_EXTENSIONS = {
    "jpg",
    "jpeg",
    "png",
    "gif",
    "svg",
    "pdf",
    "zip",
    "mp4",
    "mp3",
    "webp",
}
BLOCKED_NAMESPACES = {
    "category:",
    "file:",
    "template:",
    "portal:",
    "user:",
    "user_talk:",
    "wikipedia:",
    "help:",
    "special:",
}
MAX_URLS_PER_PAGE = 50

class URLFilterStats:
    """Lightweight counter for rejected URLs during a batch."""
    def __init__(self):
        self.non_en_wikipedia = 0
        self.namespace = 0
        self.query_params = 0
        self.extension = 0
        self.url_length = 0
        # new quality counters
        self.numeric_pages = 0
        self.year_pages = 0
        self.list_pages = 0
        self.disambiguation_pages = 0
        self.timeline_pages = 0
        self.low_quality_pages = 0
        self.other = 0

    def increment(self, reason):
        if reason == "non_en_wikipedia":
            self.non_en_wikipedia += 1
        elif reason == "namespace":
            self.namespace += 1
        elif reason == "query_params":
            self.query_params += 1
        elif reason == "extension":
            self.extension += 1
        elif reason == "url_length":
            self.url_length += 1
        elif reason == "numeric_page":
            self.numeric_pages += 1
        elif reason == "year_page":
            self.year_pages += 1
        elif reason == "list_page":
            self.list_pages += 1
        elif reason == "disambiguation":
            self.disambiguation_pages += 1
        elif reason == "timeline":
            self.timeline_pages += 1
        elif reason == "low_quality":
            self.low_quality_pages += 1
        else:
            self.other += 1

    def total(self):
        return (
            self.non_en_wikipedia
            + self.namespace
            + self.query_params
            + self.extension
            + self.url_length
            + self.numeric_pages
            + self.year_pages
            + self.list_pages
            + self.disambiguation_pages
            + self.timeline_pages
            + self.low_quality_pages
            + self.other
        )

    def to_dict(self):
        return {
            "non_en_wikipedia": self.non_en_wikipedia,
            "namespace": self.namespace,
            "query_params": self.query_params,
            "extension": self.extension,
            "url_length": self.url_length,
            "numeric_pages": self.numeric_pages,
            "year_pages": self.year_pages,
            "list_pages": self.list_pages,
            "disambiguation_pages": self.disambiguation_pages,
            "timeline_pages": self.timeline_pages,
            "low_quality_pages": self.low_quality_pages,
            "other": self.other,
        }


def should_enqueue_url(url):
    """Strict pre-enqueue filtering. Returns (allowed, reason)."""
    if not url:
        return False, "empty"

    # Check URL length
    if len(url) > MAX_URL_LENGTH:
        return False, "url_length"

    lowered = url.lower()

    # Check for query parameters
    if "?" in url:
        return False, "query_params"

    # Check extension
    for ext in BLOCKED_EXTENSIONS:
        if lowered.endswith(f".{ext}"):
            return False, "extension"

    # Check for namespace prefixes (case-insensitive)
    for ns in BLOCKED_NAMESPACES:
        if ns in lowered:
            return False, "namespace"

    # Check non-English Wikipedia
    if ".wikipedia.org" in lowered:
        if not lowered.startswith("https://en.") and not lowered.startswith(
            "http://en."
        ):
            return False, "non_en_wikipedia"

    # Additional strict quality filters
    parsed = urlparse(url)
    path = parsed.path or ""

    # pure numeric pages: /wiki/12345
    import re

    if re.match(r"^/wiki/\d+$", path):
        return False, "numeric_page"

    # year pages: /wiki/1999 or /wiki/850
    if re.match(r"^/wiki/\d{3,4}$", path):
        return False, "year_page"

    # list pages: contains List_of (case-insensitive)
    if "list_of" in lowered or "list%5fof" in lowered:
        return False, "list_page"

    # disambiguation pages
    if "(disambiguation)" in lowered or "disambiguation" in lowered and "(" in url:
        return False, "disambiguation"

    # timeline pages
    if "timeline" in lowered:
        return False, "timeline"

    # pages with very short last path segment (low quality)
    last = path.rstrip("/").split("/")[-1] if path else ""
    if last and len(last) < 3:
        return False, "low_quality"

    # excessive percent-encoding
    if url.count("%") > 5:
        return False, "low_quality"

    return True, "ok"


robots_cache = {}


def get_robots_parser_for(domain):
    robots_key = f"{config['redis']['namespace']}:robots:{domain}"
    cached = frontier.redis.get(robots_key)
    parser = robotparser.RobotFileParser()

    if cached:
        try:
            parser.parse(cached.splitlines())
            logger.info("robots_cached", extra={"domain": domain})
            return parser
        except Exception as e:
            logger.warning(
                "robots_parse_failure", extra={"domain": domain, "error": str(e)}
            )

    # Try HTTPS first, then HTTP
    for scheme in ("https", "http"):
        robots_url = f"{scheme}://{domain}/robots.txt"
        try:
            resp = requests.get(
                robots_url, timeout=5, headers={"User-Agent": "MiniSearchBot"}
            )
            if resp.status_code == 200 and resp.text:
                text = resp.text
                try:
                    parser.parse(text.splitlines())
                    # cache in redis for others
                    try:
                        frontier.redis.setex(robots_key, robots_ttl_seconds, text)
                    except Exception:
                        pass
                    logger.info(
                        "robots_fetched", extra={"domain": domain, "url": robots_url}
                    )
                    return parser
                except Exception as e:
                    logger.warning(
                        "robots_parse_failure",
                        extra={"domain": domain, "error": str(e)},
                    )
                    # fallthrough to next scheme or default allow
            else:
                # non-200 should be treated as allow (do not block)
                logger.info(
                    "robots_fetch_non_200",
                    extra={
                        "domain": domain,
                        "url": robots_url,
                        "status": resp.status_code,
                    },
                )
                break
        except Exception as e:
            logger.warning(
                "robots_fetch_failure",
                extra={"domain": domain, "error": str(e), "url": robots_url},
            )
            continue

    # default: allow everything when robots unavailable or parse failed
    try:
        parser.parse(["User-agent: *", "Disallow:"])
    except Exception:
        pass
    logger.info("robots_default_allow", extra={"domain": domain})
    return parser


def allowed_by_robots(url):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        parser = get_robots_parser_for(domain)
        # pass full URL to can_fetch for robust matching
        decision = parser.can_fetch("MiniSearchBot", url)
        return bool(decision)
    except Exception as e:
        logger.warning("robots_check_error", extra={"url": url, "error": str(e)})
        return True


def fetch_page(url, depth):
    session = get_session()
    # enforce per-domain delay
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    last = frontier.get_domain_last_access(domain)
    now = time.time()
    elapsed = now - last if last else None
    if elapsed is not None and elapsed < per_domain_delay_seconds:
        wait = per_domain_delay_seconds - elapsed
        logger.info(
            "delayed_request",
            extra={"url": url, "depth": depth, "delay_seconds": wait, "domain": domain},
        )
        time.sleep(wait)

    # robots
    if not allowed_by_robots(url):
        logger.warning(
            "robots_denied",
            extra={"url": url, "domain": domain, "depth": depth},
        )
        return None
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

    # Pre-enqueue filtering: strict stage filtering before entering queue
    filter_stats = URLFilterStats()
    next_items = []
    for link in links:
        allowed, reason = should_enqueue_url(link)
        if not allowed:
            filter_stats.increment(reason)
            continue

        # prioritization scoring: prefer article-like paths
        score = depth + 1
        lowered = link.lower()
        if is_junk_url(link):
            score += 10000
        # heuristics: /wiki/ paths without ':' are likely articles
        if "/wiki/" in lowered and ":" not in lowered:
            score -= 1
        next_items.append({"url": link, "depth": depth + 1, "score": score})
    next_items = next_items[:MAX_URLS_PER_PAGE] 
    # Log filter summary
    if filter_stats.total() > 0:
        logger.info(
            "url_filter_summary",
            extra={
                "worker_id": worker_id,
                "url": url,
                "depth": depth,
                "filtered_out": filter_stats.total(),
                **filter_stats.to_dict(),
            },
        )

    if next_items:
        next_items = next_items[:MAX_URLS_PER_PAGE] 
        res = frontier.enqueue_many(next_items, domain_budget_map=domain_budget_map)
        # Log enqueue summary instead of per-URL logs
        enqueue_summary = {
            "added": res.get("added", 0),
            "dropped": res.get("dropped", 0),
            "queue_size": frontier.queue_size(),
        }
        if res.get("dropped", 0) > 0:
            logger.warning("enqueue_summary", extra=enqueue_summary)
        pipeline.publish_urls(
            [{"url": it["url"], "depth": it.get("depth", 0)} for it in next_items]
        )

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
        # log frontier pressure
        qs = frontier.queue_size()
        if max_frontier_queue_size and qs > max_frontier_queue_size * 0.8:
            logger.warning(
                "frontier_pressure",
                extra={"queue_size": qs, "threshold": max_frontier_queue_size},
            )

        # top domain counts
        try:
            domain_stats = frontier.redis.hgetall(frontier.domain_stats_key) or {}
            top = sorted(domain_stats.items(), key=lambda kv: int(kv[1]), reverse=True)[
                :5
            ]
            logger.info("domain_counts", extra={"top_domains": top})
        except Exception:
            pass

        psutil.cpu_percent(interval=1)
        time.sleep(4)


def seed_frontier():
    if not frontier.is_empty():
        return

    seed_items = [
        {"url": clean_url(url), "depth": 0} for url in start_urls if is_valid_url(url)
    ]
    if seed_items:
        res = frontier.enqueue_many(seed_items, domain_budget_map=domain_budget_map)
        if res.get("dropped"):
            logger.warning(
                "seed_enqueue_dropped", extra={"dropped": res.get("dropped")}
            )
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
