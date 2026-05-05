import requests
from bs4 import BeautifulSoup
import time
import logging
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from urllib.parse import urljoin, urlparse
import threading
import psutil

from distributed_pipeline import DistributedSearchPipeline
from settings import load_config

config = load_config()
start_urls = config["crawler"].get("start_urls", [config["crawler"].get("start_url")])
max_pages = int(config["crawler"]["max_pages"])
num_threads = int(config["crawler"]["num_threads"])
max_depth = int(config["crawler"]["max_depth"])
retry_attempts = int(config["crawler"]["retry_attempts"])

VALID_DOMAINS = ["wikipedia.org", "arxiv.org", ".edu"]

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


# ✅ Fetch page
def fetch_page(url):
    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e} (Attempt {attempt+1})")
            time.sleep(2**attempt)
    return None


# ✅ Clean URL (remove fragments and junk)
def clean_url(url):
    """Remove fragments and trailing slashes"""
    clean = url.split("#")[0]
    if clean.endswith("/"):
        clean = clean[:-1]
    return clean


# ✅ Validate domain
def is_valid_url(url):
    """Only crawl trusted domains"""
    if not url:
        return False
    url_lower = url.lower()
    return any(domain in url_lower for domain in VALID_DOMAINS)


# ✅ Filter junk URLs
def is_junk_url(url):
    """Skip login pages, special pages, anchors"""
    junk_patterns = [
        "login",
        "signin",
        "signup",
        "register",
        "special:",
        "wikipedia:talk",
        "user:",
        "user_talk:",
        "wikipedia:sandbox",
        "edit",
        "history",
        "talk:",
    ]
    url_lower = url.lower()
    return any(pattern in url_lower for pattern in junk_patterns)


# ✅ Parse HTML
def parse_page(content, url):
    soup = BeautifulSoup(content, "html.parser")
    title = soup.title.string if soup.title else "No Title"
    text = soup.get_text()
    raw_links = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]

    # Filter: clean, validate domain, skip junk
    links = []
    for link in raw_links:
        clean = clean_url(link)
        if is_valid_url(clean) and not is_junk_url(clean):
            links.append(clean)

    return title, text, list(set(links))  # deduplicate


# ✅ Store in Elasticsearch
def store_page(pipeline, url, title, content, links):
    pipeline.index_page(url, title, content, links)


# ✅ Worker function
def crawl_page(url, depth, pipeline):
    if depth > max_depth:
        return [], 0

    logger.info(f"Fetching {url}")

    html = fetch_page(url)
    if not html:
        return [], 0

    title, content, links = parse_page(html, url)
    store_page(pipeline, url, title, content, links)

    return links, len(content)


# ✅ Monitor system
def log_system_usage():
    while True:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        logger.info(f"CPU: {cpu}% | Memory: {mem}%")
        time.sleep(5)


# ✅ MAIN CRAWLER (Distributed via Kafka)
def crawl(start_urls, max_pages, num_threads):
    pipeline = DistributedSearchPipeline(config)

    visited_urls = set()
    pending_urls = deque()

    # Publish all seed URLs to Kafka
    logger.info(f"Publishing {len(start_urls)} seed URLs to Kafka...")
    for url in start_urls:
        clean = clean_url(url)
        if is_valid_url(clean) and not is_junk_url(clean):
            pipeline.publish_urls([{"url": clean, "depth": 0}])
            pending_urls.append((clean, 0))

    total_bytes = 0
    start_time = time.time()

    # system monitor thread
    threading.Thread(target=log_system_usage, daemon=True).start()

    logger.info(
        f"Starting crawler with {num_threads} threads, target: {max_pages} pages"
    )

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = set()
        lock = threading.Lock()
        idle_rounds = 0

        while len(visited_urls) < max_pages:

            # 🔥 Consume from Kafka
            message = pipeline.next_url(timeout_ms=500)
            if message:
                url = message.get("url")
                depth = int(message.get("depth", 0))
                if url and is_valid_url(url) and not is_junk_url(url):
                    pending_urls.append((url, depth))
                idle_rounds = 0
            else:
                idle_rounds += 1

            # 🔥 Fill threads
            while pending_urls and len(futures) < num_threads:
                url, depth = pending_urls.popleft()

                with lock:
                    if url in visited_urls:
                        continue
                    visited_urls.add(url)

                logger.info(f"[{len(visited_urls)}/{max_pages}] Processing: {url}")

                future = executor.submit(crawl_page, url, depth, pipeline)
                future.depth = depth
                futures.add(future)

            # 🔥 Process completed tasks
            if futures:
                done, futures = wait(futures, timeout=0, return_when=FIRST_COMPLETED)

                for future in done:
                    links, size = future.result()
                    total_bytes += size

                    current_depth = future.depth

                    for link in links:
                        pending_urls.append((link, current_depth + 1))

            # 🔥 If nothing to do
            if not pending_urls and not futures and idle_rounds >= 3:
                logger.info("Queue empty, stopping consumer...")
                break

    pipeline.close()

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"\n✅ CRAWL COMPLETE")
    logger.info(f"Pages indexed: {len(visited_urls)}")
    logger.info(f"Total data: {total_bytes} bytes")
    logger.info(f"Time: {elapsed}s")
    logger.info(f"Rate: {round(len(visited_urls) / elapsed, 2)} pages/sec")


# ENTRY - Support both single and multiple seed URLs
if __name__ == "__main__":
    crawl(start_urls, max_pages, num_threads)
