"""API key authentication — validated against API_KEY environment variable."""

from __future__ import annotations

import os
import secrets

from fastapi import Header, HTTPException


def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> None:
    expected = os.environ.get("API_KEY", "")
    if not expected or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid API key")
