import multiprocessing
import os

workers = int(os.getenv("WORKERS", multiprocessing.cpu_count() * 2 + 1))
bind = "0.0.0.0:8000"
worker_class = "uvicorn.workers.UvicornWorker"
timeout = int(os.getenv("TIMEOUT", 30))
graceful_timeout = int(os.getenv("GRACEFUL_TIMEOUT", 30))
keepalive = 5
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")

