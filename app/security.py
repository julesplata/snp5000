from fastapi import Security
from fastapi.security.api_key import APIKeyHeader

# Optional API key header for OpenAPI security scheme registration.
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)


async def get_api_key_optional(api_key: str = Security(api_key_header)):
    return api_key
