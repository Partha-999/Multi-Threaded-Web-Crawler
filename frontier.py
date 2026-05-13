import json
import time
from urllib.parse import urlsplit, urlunsplit

import redis


class URLFrontier:
    def __init__(
        self,
        redis_url="redis://localhost:6379/0",
        namespace="crawler",
        config=None,
    ):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.queue_key = f"{namespace}:queue"
        self.queue_zkey = f"{namespace}:queue_z"
        self.visited_key = f"{namespace}:visited"
        self.discovered_key = f"{namespace}:discovered"
        self.inflight_key = f"{namespace}:inflight"
        self.domain_stats_key = f"{namespace}:domain_stats"
        self.domain_last_key = f"{namespace}:domain_last"
        self.robots_key_prefix = f"{namespace}:robots"

        cfg = config or {}
        self.lease_seconds = int(cfg.get("lease_seconds", 300))
        self.priority_enabled = bool(cfg.get("priority_enabled", True))
        self.max_frontier_queue_size = int(cfg.get("max_frontier_queue_size", 200000))
        self.domain_max_requests_default = int(
            cfg.get("domain_max_requests_default", 1000)
        )

        # Reserve script: pop from zset (lowest score) if present, else list LPOP
        self._reserve_script = self.redis.register_script("""
local zkey = KEYS[1]
local lkey = KEYS[2]
local inflight = KEYS[3]
local meta = ARGV[1]

local res = nil
local zitems = redis.call('ZRANGE', zkey, 0, 0)
if zitems and #zitems > 0 then
  res = zitems[1]
  redis.call('ZREM', zkey, res)
else
  res = redis.call('LPOP', lkey)
end

if not res then
  return nil
end

redis.call('HSET', inflight, res, meta)
return res
""")
        self.redis.ping()

    def normalize_url(self, url):
        if not url:
            return ""

        parsed = urlsplit(url)
        normalized = urlunsplit(parsed._replace(fragment=""))
        return normalized.rstrip("/")

    def _domain_from_url(self, url):
        try:
            return urlsplit(url).netloc.lower()
        except Exception:
            return ""

    def enqueue(self, url, depth=0, score=None, domain_budget_map=None):
        """Enqueue a single url. Returns (status, message).

        status: 'added', 'exists', 'visited', 'throttled', 'dropped', 'domain_budget_exceeded'
        """
        normalized = self.normalize_url(url)
        if not normalized:
            return "dropped", "empty_url"

        if self.redis.sismember(self.visited_key, normalized):
            return "visited", "already_visited"

        if self.redis.sismember(self.discovered_key, normalized):
            return "exists", "already_discovered"

        # Frontier pressure control
        if self.queue_size() >= self.max_frontier_queue_size:
            return "throttled", "queue_pressure"

        # Domain budgeting
        domain = self._domain_from_url(normalized)
        domain_count = int(self.redis.hget(self.domain_stats_key, domain) or 0)
        domain_max = None
        if domain_budget_map and domain in domain_budget_map:
            try:
                domain_max = int(domain_budget_map.get(domain))
            except Exception:
                domain_max = self.domain_max_requests_default
        else:
            domain_max = self.domain_max_requests_default

        if domain_count >= domain_max:
            return "domain_budget_exceeded", "domain_limit"

        payload = json.dumps({"url": normalized, "depth": int(depth)})

        # push to priority zset or list
        if self.priority_enabled and score is not None:
            # lower score = higher priority
            self.redis.zadd(self.queue_zkey, {payload: float(score)})
        else:
            self.redis.rpush(self.queue_key, payload)

        self.redis.sadd(self.discovered_key, normalized)
        return "added", "ok"

    def enqueue_many(self, items, domain_budget_map=None):
        added = 0
        dropped = 0
        details = []
        for it in items:
            if isinstance(it, dict):
                url = it.get("url")
                depth = it.get("depth", 0)
                score = it.get("score", None)
            else:
                url = it
                depth = 0
                score = None

            status, msg = self.enqueue(
                url, depth=depth, score=score, domain_budget_map=domain_budget_map
            )
            details.append((url, status, msg))
            if status == "added":
                added += 1
            else:
                dropped += 1

        return {"added": added, "dropped": dropped, "details": details}

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
            keys=[self.queue_zkey, self.queue_key, self.inflight_key], args=[meta_json]
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
        try:
            self.redis.hdel(self.inflight_key, payload)
        except Exception:
            pass
        if url:
            nurl = self.normalize_url(url)
            self.redis.sadd(self.visited_key, nurl)
            # increment domain count and record last access
            domain = self._domain_from_url(nurl)
            self.redis.hincrby(self.domain_stats_key, domain, 1)
            self.redis.hset(self.domain_last_key, domain, time.time())

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
                # push back to zset at default score(depth)
                if self.priority_enabled:
                    pipe.zadd(
                        self.queue_zkey,
                        {json.dumps({"url": url, "depth": depth}): float(depth)},
                    )
                else:
                    pipe.rpush(self.queue_key, json.dumps({"url": url, "depth": depth}))
            pipe.execute()

    def is_visited(self, url):
        return self.redis.sismember(self.visited_key, self.normalize_url(url))

    def queue_size(self):
        # combine zset and list sizes
        z = int(self.redis.zcard(self.queue_zkey))
        l = int(self.redis.llen(self.queue_key))
        return z + l

    def inflight_size(self):
        return int(self.redis.hlen(self.inflight_key))

    def visited_count(self):
        return int(self.redis.scard(self.visited_key))

    def is_empty(self):
        return self.queue_size() == 0 and self.inflight_size() == 0

    def get_domain_last_access(self, domain):
        v = self.redis.hget(self.domain_last_key, domain)
        try:
            return float(v) if v else 0.0
        except Exception:
            return 0.0

    def get_domain_count(self, domain):
        return int(self.redis.hget(self.domain_stats_key, domain) or 0)

    def close(self):
        self.redis.close()
