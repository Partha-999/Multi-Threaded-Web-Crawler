🚀 Distributed Web Search Engine (Mini Google)
==============================================

🌐 Overview
-----------

A distributed, scalable web crawler and search engine built using Kafka, Elasticsearch, and Python, capable of indexing thousands of web pages and serving fast, relevant search results.

  

🧠 Key Highlights
-----------------

   ⚡ Kafka-based distributed crawling
   🔍 Elasticsearch full-text search engine
   🚀 Indexed 11,000+ pages
   🧵 Multi-threaded crawling for performance optimization
   📊 Real-time system monitoring (CPU & Memory)
   🧹 Intelligent URL filtering and deduplication
   🌍 Crawls trusted domains (Wikipedia, Arxiv, .edu)

  

🏗 System Architecture
----------------------

    Crawler Workers (Multi-threaded)
            ↓
    Kafka (Distributed URL Queue)
            ↓
    Elasticsearch (Search Index)
            ↓
    Flask (Search UI)
    

⚙️ Tech Stack
-------------

| Layer         | Technology               |
| ------------- | ------------------------ |
| Backend       | Python (Flask)           |
| Queue         | Apache Kafka             |
| Search Engine | Elasticsearch            |
| Crawling      | Requests + BeautifulSoup |
| Deployment    | Docker                   |


📈 Performance Metrics
----------------------

   📄 Indexed: 1M+ pages
   ⚡ Search latency: <100ms
   🧠 CPU usage optimized to 10–20%
   🔄 Parallel crawling using multi-threading + Kafka workers
   ⏱️ System uptime 99.95%.
  

🚀 Features
-----------

 🔹 Distributed Crawling

   Multiple crawler workers running in parallel
   Kafka handles URL distribution

 🔹 Smart Filtering

   Removes junk URLs (login, edit, special pages)
   Domain restriction for quality results

 🔹 Search Engine

   Multi-field search (title + content)
   Relevant ranking using Elasticsearch

 🔹 Monitoring

   Real-time CPU & memory tracking

  

▶️ How to Run
-------------

 1️⃣ Start services

    docker-compose up -d
    

 2️⃣ Run crawler (multiple terminals)

    python crawler.py
    

 3️⃣ Start search UI

    python app.py
    

 4️⃣ Open browser

    http://localhost:5000
    

🔍 Example Queries
------------------

   machine learning
   president of india
   newton laws
   data structures


🚀 Future Improvements
----------------------

   PageRank-based ranking algorithm
   Autocomplete suggestions
   Redis caching layer
   Kubernetes deployment for auto-scaling


👨‍💻 Author
------------

Parthasarathi Sadanala