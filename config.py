from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from typing import List
from pydantic import field_validator
import json


class Settings(BaseSettings):
    alpaca_secret: str
    alpaca_key: str
    fred_api_key: str
    finnhub_api_key: str
    database_url: str
    redis_url: str | None = None
    log_level: str = "INFO"
    allowed_origins: list[str] = ["http://localhost:3000", "http://localhost:5173"]
    # Default to 1 worker for low-memory deploy tiers; override via env WORKERS
    workers: int = 1
    rate_limit_max_requests: int = 60
    rate_limit_window_seconds: int = 60

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="allow"
    )

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def parse_allowed_origins(cls, v):
        """
        Allow ALLOWED_ORIGINS to be provided as a JSON array or comma-separated string.
        """
        if isinstance(v, str):
            raw = v.strip()
            if raw.startswith("["):
                # Try JSON array, e.g. ["https://a","https://b"]
                try:
                    loaded = json.loads(raw)
                    if isinstance(loaded, list):
                        return [str(item).strip() for item in loaded if str(item).strip()]
                except Exception:
                    # fall through to comma parsing if JSON fails
                    pass
            # Accept comma separated values
            return [origin.strip() for origin in raw.split(",") if origin.strip()]
        return v


@lru_cache
def get_settings():
    return Settings()
