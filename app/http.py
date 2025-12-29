from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests


class HttpError(RuntimeError):
    pass


def request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    retries: int = 8,
) -> Dict[str, Any]:
    """
    Универсальный запрос с ретраями.
    ВАЖНО: для MoySklad часто ловим 429 — делаем backoff.
    """
    last_exc: Optional[BaseException] = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
        except Exception as e:
            last_exc = e
            time.sleep(min(2 ** attempt, 20))
            continue

        text = resp.text or ""

        # Ретраи на лимит/временные
        if resp.status_code in (429, 500, 502, 503, 504):
            # мягкий backoff
            sleep_s = min(2 ** attempt, 25)
            time.sleep(sleep_s)
            continue

        if resp.status_code >= 400:
            raise HttpError(f"{resp.status_code} {url} -> {text[:1500]}")

        if not text.strip():
            return {}

        try:
            return resp.json()
        except json.JSONDecodeError as e:
            raise HttpError(f"Invalid JSON from {url}: {text[:1500]}") from e

    raise HttpError(f"HTTP request failed after retries: {last_exc}")
