from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from elasticsearch import Elasticsearch
import psutil
import time
import requests
from bs4 import BeautifulSoup
from threading import Thread

# Initialize the Flask app and Flask-SocketIO
app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app)  # Remove async_mode='threading'

# Initialize Elasticsearch client
try:
    es = Elasticsearch(hosts=["http://localhost:9200"])
    es.info()
except Exception as e:
    print(f"Warning: Could not connect to Elasticsearch: {e}")
    es = None


# Function to fetch sublinks from the given URL
def get_sublinks(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all the anchor tags (<a>) and extract the href attributes
        links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.startswith("http"):
                links.append(href)
            else:
                links.append(url + href)
        return links
    except Exception as e:
        print("Error fetching sublinks:", e)
        return []


# Function to log system usage (CPU and memory) every second
def log_system_usage():
    while True:
        # Fetch CPU and memory usage
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent

        # Emit the data to the frontend using SocketIO
        socketio.emit("system_usage", {"cpu": cpu_percent, "memory": memory_percent})

        # Sleep for 1 second before the next check
        time.sleep(1)


# Function to start the system usage logging in a separate thread
def start_logging():
    system_usage_thread = Thread(target=log_system_usage)
    system_usage_thread.daemon = True
    system_usage_thread.start()


# Initialize the logging of system usage
start_logging()

TRUSTED_DOMAINS = ["wikipedia.org", "arxiv.org"]

def is_trusted_domain(url):
    """Check if URL is from a trusted domain"""
    if not url:
        return False
    url_lower = url.lower()
    if any(domain in url_lower for domain in TRUSTED_DOMAINS):
        return True
    if ".edu" in url_lower:
        return True
    return False

def get_domain(url):
    """Extract domain from URL"""
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        return domain.lower()
    except:
        return ""

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search")
def search():
    query = request.args.get("q", "").strip()
    results = []

    if not query or not es:
        return render_template(
            "results.html", query=query, results=results, error=not es
        )

    try:
        response = es.search(
            index="web_pages",
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["title^2", "content"],
                    "fuzziness": "AUTO"
                }
            },
            size=50,
        )

        seen_urls = set()
        for hit in response.get("hits", {}).get("hits", []):
            doc = hit["_source"]
            url = doc.get("url", "").split("#")[0]
            
            if not url or not is_trusted_domain(url):
                continue
            
            if url in seen_urls:
                continue
            
            seen_urls.add(url)
            
            results.append(
                {
                    "title": doc.get("title", "No Title"),
                    "url": url,
                    "preview": (
                        (doc.get("content", "")[:200] + "...")
                        if len(doc.get("content", "")) > 200
                        else doc.get("content", "")
                    ),
                }
            )
            
            if len(results) >= 10:
                break
                
    except Exception as e:
        print(f"Search error: {e}")
        return render_template("results.html", query=query, results=[], error=True)

    return render_template("results.html", query=query, results=results, error=False)


@app.route("/get_sublinks")
def fetch_sublinks():
    url = request.args.get("url")
    sublinks = get_sublinks(url)
    return jsonify({"sublinks": sublinks})


@socketio.on("connect")
def handle_connect():
    print("Client connected")


@socketio.on("disconnect")
def handle_disconnect():
    print("Client disconnected")


if __name__ == "__main__":
    # Run the Flask app with SocketIO
    socketio.run(app, debug=True)
