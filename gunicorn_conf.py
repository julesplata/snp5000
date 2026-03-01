import multiprocessing
import os

# Keep memory low on small tiers: default to 1, otherwise cap to CPU*2+1
default_workers = 1
auto_workers = multiprocessing.cpu_count() * 2 + 1
workers = int(os.getenv("WORKERS", default_workers))
workers = max(1, min(workers, auto_workers))

bind_port = os.getenv("PORT", "8000")
bind = f"0.0.0.0:{bind_port}"
worker_class = "uvicorn.workers.UvicornWorker"
timeout = int(os.getenv("TIMEOUT", 30))
graceful_timeout = int(os.getenv("GRACEFUL_TIMEOUT", 30))
keepalive = 5
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")
