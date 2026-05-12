from datetime import datetime
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaProducer


class DistributedSearchPipeline:
    def __init__(self, config):
        self.kafka_config = config["kafka"]
        self.es_config = config["elasticsearch"]

        self.kafka_topic = self.kafka_config["topic"]
        self.kafka_batch_size = int(self.kafka_config.get("batch_size", 25))
        self.index_name = self.es_config["index"]
        self.bulk_size = int(self.es_config.get("bulk_size", 500))

        self.es = Elasticsearch(self.es_config["hosts"], request_timeout=30)
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            linger_ms=100,
            acks="all",
        )

        self.url_buffer = []
        self.document_buffer = []

        self.create_index()

    def create_index(self):
        if self.es.indices.exists(index=self.index_name):
            return

        self.es.indices.create(
            index=self.index_name,
            mappings={
                "properties": {
                    "url": {"type": "keyword"},
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "links": {"type": "keyword"},
                    "fetched_at": {"type": "date"},
                }
            },
        )

    def publish_urls(self, urls):
        for item in urls:
            payload = item if isinstance(item, dict) else {"url": item, "depth": 0}
            self.url_buffer.append(payload)

            if len(self.url_buffer) >= self.kafka_batch_size:
                self.flush_urls()

    def flush_urls(self):
        if not self.url_buffer:
            return

        while self.url_buffer:
            batch = self.url_buffer[: self.kafka_batch_size]
            self.url_buffer = self.url_buffer[self.kafka_batch_size :]
            self.producer.send(
                self.kafka_topic,
                {
                    "items": batch,
                    "batch_size": len(batch),
                    "published_at": datetime.utcnow().isoformat(),
                },
            )

        self.producer.flush()

    def index_page(self, url, title, content, links):
        self.document_buffer.append(
            {
                "_index": self.index_name,
                "_id": url,
                "_source": {
                    "url": url,
                    "title": title,
                    "content": content,
                    "links": links,
                    "fetched_at": datetime.utcnow().isoformat(),
                },
            }
        )

        if len(self.document_buffer) >= self.bulk_size:
            self.flush_bulk()

    def flush_bulk(self):
        if not self.document_buffer:
            return

        bulk(self.es, self.document_buffer, refresh=False)
        self.document_buffer = []

    def close(self):
        self.flush_urls()
        self.flush_bulk()
        self.producer.close()
        self.es.close()
