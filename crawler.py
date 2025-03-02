import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
import yaml
import threading
import os
from urllib.robotparser import RobotFileParser
import psutil  # New import for system monitoring

# Load configuration
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

start_url = config["start_url"]
max_pages = config["max_pages"]
num_threads = config["num_threads"]
max_depth = config["max_depth"]
retry_attempts = int(config["retry_attempts"])

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Database setup
def setup_database():
    conn = sqlite3.connect('web_crawler.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY,
            url TEXT UNIQUE,
            title TEXT,
            content TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Fetching page content with retry logic
def fetch_page(url):
    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}. Attempt {attempt + 1} of {retry_attempts}")
            time.sleep(2 ** attempt)  # Exponential backoff
    return None

# Parsing page content
def parse_page(content, url):
    soup = BeautifulSoup(content, 'html.parser')
    title = soup.title.string if soup.title else 'No Title'
    page_text = soup.get_text()
    links = [urljoin(url, link['href']) for link in soup.find_all('a', href=True)]
    return title, page_text, links

# Storing page content
def store_page(url, title, content):
    conn = sqlite3.connect('web_crawler.db')
    c = conn.cursor()
    try:
        c.execute('INSERT INTO pages (url, title, content) VALUES (?, ?, ?)', (url, title, content))
        conn.commit()
    except sqlite3.IntegrityError:
        logger.info(f"URL already exists in the database: {url}")
    finally:
        conn.close()

# Checking robots.txt
def is_allowed(url):
    parsed_url = urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except:
        return False
    return rp.can_fetch("*", url)

# Main crawl function
def crawl_page(url, visited_urls, lock, depth):
    if url in visited_urls or depth > max_depth or not is_allowed(url):
        return [], 0

    logger.info(f"Fetching {url}")
    page_content = fetch_page(url)
    if page_content:
        title, content, links = parse_page(page_content, url)
        store_page(url, title, content)
        with lock:
            visited_urls.add(url)
        return links, len(content)
    return [], 0

def send_system_usage(cpu_usage, memory_usage):
    try:
        requests.post('http://localhost:5000/system_usage', json={'cpu': cpu_usage, 'memory': memory_usage})
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending system usage data: {e}")


# Function to monitor and log system usage
def log_system_usage():
    while True:
        cpu_usage = psutil.cpu_percent(interval=1)
        memory_usage = psutil.virtual_memory().percent
        logger.info(f"CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
        send_system_usage(cpu_usage, memory_usage)  # Send data to Flask server
        time.sleep(5)

def crawl(start_url, max_pages, num_threads):
    setup_database()
    visited_urls = set()
    to_visit_urls = [(start_url, 0)]
    total_bytes = 0
    start_time = time.time()

    # Start system usage monitoring in a separate thread
    monitoring_thread = threading.Thread(target=log_system_usage)
    monitoring_thread.daemon = True
    monitoring_thread.start()
    
    # Using ThreadPoolExecutor to manage a pool of threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        lock = threading.Lock()
        
        while to_visit_urls and len(visited_urls) < max_pages:
            while to_visit_urls and len(futures) < num_threads:
                current_url, depth = to_visit_urls.pop(0)
                # Submit crawl_page function as a task to the thread pool
                futures.append(executor.submit(crawl_page, current_url, visited_urls, lock, depth))
                
            for future in as_completed(futures):
                # Process results as soon as they are available
                links, bytes_fetched = future.result()
                total_bytes += bytes_fetched
                for link in links:
                    to_visit_urls.append((link, depth + 1))
                futures.remove(future)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Crawling finished. Pages crawled: {len(visited_urls)}, Total bytes fetched: {total_bytes}, Time taken: {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    crawl(start_url, max_pages, num_threads)