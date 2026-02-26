import time
import asyncio
from collections import defaultdict, deque
from typing import Deque, DefaultDict

from fastapi import HTTPException, Request


class InMemoryRateLimiter:
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


# Default instance: 60 requests per minute per client IP
rate_limiter = InMemoryRateLimiter(max_requests=60, window_seconds=60)
