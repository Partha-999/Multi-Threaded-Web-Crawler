from datetime import datetime
import json
import logging
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaProducer


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key in (
            "bulk_size",
            "indexed_count",
            "failed_count",
            "bulk_latency_ms",
            "exception_type",
            "exception_msg",
            "retry_attempt",
        ):
            value = record.__dict__.get(key)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
logger.handlers = [handler]
logger.propagate = False


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
        """Create index if it doesn't exist. Log but don't crash on errors."""
        try:
            if self.es.indices.exists(index=self.index_name):
                logger.info("index_already_exists", extra={"index": self.index_name})
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
            logger.info("index_created", extra={"index": self.index_name})

        except Exception as e:
            logger.warning(
                "index_creation_error",
                extra={
                    "index": self.index_name,
                    "exception_type": type(e).__name__,
                    "exception_msg": str(e),
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

    def flush_bulk(self, max_retries=3):
        """Flush document buffer to Elasticsearch with retry and exception handling.

        Never terminates on failure - logs and continues.
        """
        if not self.document_buffer:
            return

        buffer_size = len(self.document_buffer)
        indexed_count = 0
        failed_count = 0

        for attempt in range(max_retries):
            try:
                start_time = time.time()

                # bulk() returns (success_count, errors_list)
                success_count, errors = bulk(
                    self.es,
                    self.document_buffer,
                    refresh=False,
                    request_timeout=30,
                    chunk_size=self.bulk_size,
                )

                elapsed_ms = int((time.time() - start_time) * 1000)
                indexed_count = success_count
                failed_count = len(errors) if errors else 0

                # Log successful write
                logger.info(
                    "bulk_write_success",
                    extra={
                        "bulk_size": buffer_size,
                        "indexed_count": indexed_count,
                        "failed_count": failed_count,
                        "bulk_latency_ms": elapsed_ms,
                    },
                )

                # Log any partial failures
                if failed_count > 0:
                    logger.warning(
                        "bulk_write_partial_failure",
                        extra={
                            "bulk_size": buffer_size,
                            "indexed_count": indexed_count,
                            "failed_count": failed_count,
                            "bulk_latency_ms": elapsed_ms,
                        },
                    )

                self.document_buffer = []
                return

            except Exception as e:
                elapsed_ms = int((time.time() - start_time) * 1000)
                is_last = attempt == max_retries - 1

                logger.warning(
                    "bulk_write_elasticsearch_error",
                    extra={
                        "bulk_size": buffer_size,
                        "failed_count": buffer_size,
                        "bulk_latency_ms": elapsed_ms,
                        "exception_type": type(e).__name__,
                        "exception_msg": str(e),
                        "retry_attempt": attempt + 1,
                        "max_retries": max_retries,
                    },
                )

                if is_last:
                    # Final attempt failed - log and drop buffer but don't crash
                    logger.error(
                        "bulk_write_final_failure",
                        extra={
                            "bulk_size": buffer_size,
                            "exception_type": type(e).__name__,
                            "exception_msg": str(e),
                            "action": "dropping_buffer",
                        },
                    )
                    self.document_buffer = []
                    return

                # Exponential backoff: 1s, 2s, 4s
                wait_time = 2**attempt
                time.sleep(wait_time)

            except Exception as e:
                elapsed_ms = int((time.time() - start_time) * 1000)
                is_last = attempt == max_retries - 1

                logger.warning(
                    "bulk_write_generic_error",
                    extra={
                        "bulk_size": buffer_size,
                        "failed_count": buffer_size,
                        "bulk_latency_ms": elapsed_ms,
                        "exception_type": type(e).__name__,
                        "exception_msg": str(e),
                        "retry_attempt": attempt + 1,
                        "max_retries": max_retries,
                    },
                )

                if is_last:
                    # Final attempt failed - log and drop buffer but don't crash
                    logger.error(
                        "bulk_write_final_failure",
                        extra={
                            "bulk_size": buffer_size,
                            "exception_type": type(e).__name__,
                            "exception_msg": str(e),
                            "action": "dropping_buffer",
                        },
                    )
                    self.document_buffer = []
                    return

                # Exponential backoff: 1s, 2s, 4s
                wait_time = 2**attempt
                time.sleep(wait_time)

    def close(self):
        """Gracefully close pipeline, handling any flush errors."""
        # Flush any remaining data - don't crash if it fails
        try:
            self.flush_urls()
        except Exception as e:
            logger.warning(
                "flush_urls_error_on_close",
                extra={"exception_type": type(e).__name__, "exception_msg": str(e)},
            )

        try:
            self.flush_bulk()
        except Exception as e:
            logger.warning(
                "flush_bulk_error_on_close",
                extra={"exception_type": type(e).__name__, "exception_msg": str(e)},
            )

        # Close connections - don't crash if they fail
        try:
            self.producer.close()
        except Exception as e:
            logger.warning(
                "producer_close_error",
                extra={"exception_type": type(e).__name__, "exception_msg": str(e)},
            )

        try:
            self.es.close()
        except Exception as e:
            logger.warning(
                "elasticsearch_close_error",
                extra={"exception_type": type(e).__name__, "exception_msg": str(e)},
            )
