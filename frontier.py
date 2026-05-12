import json
import time
from urllib.parse import urlsplit, urlunsplit

import redis


class URLFrontier:
    def __init__(self, redis_url="redis://localhost:6379/0", namespace="crawler"):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.queue_key = f"{namespace}:queue"
        self.visited_key = f"{namespace}:visited"
        self.discovered_key = f"{namespace}:discovered"
        self.inflight_key = f"{namespace}:inflight"
        self.lease_seconds = 300
        self._reserve_script = self.redis.register_script("""
local queue_key = KEYS[1]
local inflight_key = KEYS[2]
local meta_json = ARGV[1]

local item = redis.call('LPOP', queue_key)
if not item then
    return nil
end

redis.call('HSET', inflight_key, item, meta_json)
return item
""")
        self.redis.ping()

    def normalize_url(self, url):
        if not url:
            return ""

        parsed = urlsplit(url)
        normalized = urlunsplit(parsed._replace(fragment=""))
        return normalized.rstrip("/")

    def enqueue(self, url, depth=0):
        normalized = self.normalize_url(url)
        if not normalized:
            return False

        if self.redis.sismember(self.visited_key, normalized):
            return False

        if self.redis.sadd(self.discovered_key, normalized) == 1:
            payload = json.dumps({"url": normalized, "depth": int(depth)})
            self.redis.rpush(self.queue_key, payload)
            return True

        return False

    def enqueue_many(self, urls):
        added = 0
        for item in urls:
            if isinstance(item, dict):
                url = item.get("url")
                depth = item.get("depth", 0)
            else:
                url = item
                depth = 0

            if self.enqueue(url, depth):
                added += 1

        return added

    def reserve(self, worker_id, lease_seconds=None):
        lease_seconds = int(lease_seconds or self.lease_seconds)
        meta_json = json.dumps(
            {
                "worker_id": worker_id,
                "reserved_at": time.time(),
                "lease_seconds": lease_seconds,
            }
        )
        payload = self._reserve_script(
            keys=[self.queue_key, self.inflight_key], args=[meta_json]
        )
        if not payload:
            return None

        item = json.loads(payload)
        item["_payload"] = payload
        return item

    def reserve_batch(self, worker_id, batch_size=25, lease_seconds=None):
        batch = []
        for _ in range(int(batch_size)):
            item = self.reserve(worker_id, lease_seconds=lease_seconds)
            if item is None:
                break
            batch.append(item)
        return batch

    def acknowledge(self, payload, url=None):
        self.redis.hdel(self.inflight_key, payload)
        if url:
            self.redis.sadd(self.visited_key, self.normalize_url(url))

    def requeue_stale(self, max_age_seconds=None):
        max_age_seconds = int(max_age_seconds or self.lease_seconds)
        now = time.time()
        inflight_items = self.redis.hgetall(self.inflight_key)

        for payload, meta_json in inflight_items.items():
            try:
                meta = json.loads(meta_json)
                reserved_at = float(meta.get("reserved_at", now))
            except Exception:
                reserved_at = now

            if now - reserved_at < max_age_seconds:
                continue

            try:
                data = json.loads(payload)
                url = self.normalize_url(data.get("url"))
                depth = int(data.get("depth", 0))
            except Exception:
                url = ""
                depth = 0

            pipe = self.redis.pipeline()
            pipe.hdel(self.inflight_key, payload)
            if url and not self.redis.sismember(self.visited_key, url):
                pipe.rpush(self.queue_key, json.dumps({"url": url, "depth": depth}))
            pipe.execute()

    def is_visited(self, url):
        return self.redis.sismember(self.visited_key, self.normalize_url(url))

    def queue_size(self):
        return int(self.redis.llen(self.queue_key))

    def inflight_size(self):
        return int(self.redis.hlen(self.inflight_key))

    def visited_count(self):
        return int(self.redis.scard(self.visited_key))

    def is_empty(self):
        return self.queue_size() == 0 and self.inflight_size() == 0

    def close(self):
        self.redis.close()
