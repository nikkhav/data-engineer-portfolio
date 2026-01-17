"""
GigaChat API client.

Responsibilities:
- obtain access token via OAuth
- cache token in memory
- send prompts to GigaChat
"""

from __future__ import annotations

import os
import time
import uuid
import requests
from typing import Optional


# -----------------------------
# Configuration
# -----------------------------

OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_API_BASE = "https://gigachat.devices.sberbank.ru/api/v1"

AUTHORIZATION_KEY = os.getenv("GIGACHAT_AUTH_KEY")
SCOPE = "GIGACHAT_API_PERS"
MODEL_NAME = "GigaChat-2"

TOKEN_TTL_SECONDS = 30 * 60  # 30 minutes


# -----------------------------
# Token cache
# -----------------------------

_access_token: Optional[str] = None
_token_expires_at: float = 0.0


# -----------------------------
# OAuth
# -----------------------------

def _get_access_token() -> str:
    global _access_token, _token_expires_at

    now = time.time()

    if _access_token and now < _token_expires_at:
        return _access_token

    if not AUTHORIZATION_KEY:
        raise RuntimeError("GIGACHAT_AUTH_KEY environment variable is not set")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "RqUID": str(uuid.uuid4()),
        "Authorization": f"Basic {AUTHORIZATION_KEY}",
    }

    payload = {
        "scope": SCOPE,
    }

    response = requests.post(
        OAUTH_URL,
        headers=headers,
        data=payload,
        timeout=30,
        verify=False
    )

    response.raise_for_status()

    data = response.json()

    _access_token = data["access_token"]
    _token_expires_at = now + TOKEN_TTL_SECONDS - 10  # safety margin

    return _access_token


# -----------------------------
# Public API
# -----------------------------

def generate(prompt: str) -> str:
    """
    Send prompt to GigaChat and return model response text.
    """

    token = _get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2,
    }

    response = requests.post(
        f"{GIGACHAT_API_BASE}/chat/completions",
        headers=headers,
        json=payload,
        timeout=60,
        verify=False
    )

    response.raise_for_status()

    data = response.json()

    return data["choices"][0]["message"]["content"]
