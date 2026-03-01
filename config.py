from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from typing import List, Union
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
    # Accepts list or string from env; validator normalizes to list[str]
    allowed_origins: Union[list[str], str] = ["http://localhost:3000", "http://localhost:5173"]
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
        default = ["http://localhost:3000", "http://localhost:5173","https://972f2674.frontend-dfs.pages.dev"]
        try:
            if isinstance(v, str):
                raw = v.strip()
                # Remove optional wrapping quotes around the whole string
                if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
                    raw = raw[1:-1]
                if raw.startswith("["):
                    # Try JSON array, e.g. ["https://a","https://b"]
                    loaded = json.loads(raw)
                    if isinstance(loaded, list):
                        parsed = [
                            str(item).strip().strip('\"').strip("'")
                            for item in loaded
                            if str(item).strip()
                        ]
                        return parsed or default
                # Accept comma separated values
                parsed = [
                    origin.strip().strip('\"').strip("'")
                    for origin in raw.split(",")
                    if origin.strip()
                ]
                return parsed or default
            if isinstance(v, list):
                parsed = [
                    str(item).strip().strip('\"').strip("'")
                    for item in v
                    if str(item).strip()
                ]
                return parsed or default
        except Exception:
            return default
        return default


@lru_cache
def get_settings():
    return Settings()
