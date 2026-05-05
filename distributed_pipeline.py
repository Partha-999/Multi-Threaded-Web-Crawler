from datetime import datetime
import json

try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:
    KafkaConsumer = None
    KafkaProducer = None


class DistributedSearchPipeline:
    def __init__(self, config):
        if Elasticsearch is None or KafkaConsumer is None or KafkaProducer is None:
            raise RuntimeError(
                "Install elasticsearch and kafka-python to run the distributed pipeline."
            )

        self.kafka_config = config["kafka"]
        self.elasticsearch_config = config["elasticsearch"]
        self.topic = self.kafka_config["topic"]
        self.index_name = self.elasticsearch_config["index"]
        self.es = Elasticsearch(
            [self.elasticsearch_config["hosts"]], request_timeout=30, verify_certs=False
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        self.ensure_index()

    def ensure_index(self):
        try:
            if not self.es.indices.exists(index=self.index_name):
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
        except Exception as e:
            print("Index check failed, creating index anyway...")
        self.es.indices.create(
            index=self.index_name, ignore=400  # ignore "already exists"
        )

    def index_page(self, url, title, content, links):
        self.es.index(
            index=self.index_name,
            id=url,
            document={
                "url": url,
                "title": title,
                "content": content,
                "links": links,
                "fetched_at": datetime.utcnow().isoformat(),
            },
            refresh=False,
        )

    def publish_urls(self, urls):
        for url in urls:
            if not url:
                continue
            if isinstance(url, dict):
                payload = url
            else:
                payload = {"url": url, "depth": 0}
            self.producer.send(self.topic, value=payload)
        self.producer.flush()

    def next_url(self, timeout_ms=1000):
        records = self.consumer.poll(timeout_ms=timeout_ms)

        for partition_records in records.values():
            for record in partition_records:
                try:
                    data = record.value
                    if isinstance(data, str):
                        data = json.loads(data)
                    return data
                except Exception as e:
                    print("Error parsing message:", e)

        return None

    def close(self):
        self.consumer.close()
        self.producer.close()
