from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    alpaca_secret: str
    alpaca_key: str
    fred_api_key: str
    finnhub_api_key: str
    database_url: str
    redis_url: str | None = None
    log_level: str = "INFO"
    allowed_origins: list[str] = ["http://localhost:3000", "http://localhost:5173"]
    workers: int = 4
    rate_limit_max_requests: int = 60
    rate_limit_window_seconds: int = 60

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="allow"
    )


@lru_cache
def get_settings():
    return Settings()
