import time
import asyncio
from collections import defaultdict, deque
from typing import Deque, DefaultDict, Optional

from fastapi import HTTPException, Request

try:
    from redis.asyncio import Redis
except ImportError:  # pragma: no cover - redis not installed
    Redis = None  # type: ignore


class BaseRateLimiter:
    async def __call__(self, request: Request):
        raise NotImplementedError


class RedisRateLimiter(BaseRateLimiter):
    """
    Shared rate limiter using Redis.
    Sliding window approximation with INCR+EXPIRE per client IP.
    """

    def __init__(
        self,
        redis: Redis,
        max_requests: int = 60,
        window_seconds: int = 60,
        prefix: str = "ratelimit",
    ):
        self.redis = redis
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.prefix = prefix

    async def __call__(self, request: Request):
        client_ip = request.client.host if request.client else "anonymous"
        key = f"{self.prefix}:{client_ip}"
        pipe = self.redis.pipeline()
        pipe.incr(key, 1)
        pipe.expire(key, self.window_seconds)
        current, _ = await pipe.execute()
        if current and int(current) > self.max_requests:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")


class InMemoryRateLimiter(BaseRateLimiter):
    """
    Lightweight in-process rate limiter.
    Uses a sliding window per client IP: max_requests within window_seconds.
    Note: per-process only; for multi-instance deployments use Redis-backed limiter instead.
    """

    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: DefaultDict[str, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def __call__(self, request: Request):
        client_ip = request.client.host if request.client else "anonymous"
        now = time.time()
        async with self._lock:
            window = self._hits[client_ip]
            # drop expired hits
            while window and now - window[0] > self.window_seconds:
                window.popleft()
            if len(window) >= self.max_requests:
                raise HTTPException(status_code=429, detail="Rate limit exceeded")
            window.append(now)


# Factory to select limiter at import time
def build_rate_limiter(
    redis_client: Optional[Redis],
    max_requests: int,
    window_seconds: int,
):
    if redis_client:
        return RedisRateLimiter(
            redis=redis_client,
            max_requests=max_requests,
            window_seconds=window_seconds,
        )
    return InMemoryRateLimiter(max_requests=max_requests, window_seconds=window_seconds)

