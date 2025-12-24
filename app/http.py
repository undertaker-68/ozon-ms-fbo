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
    timeout: int = 60,
) -> Dict[str, Any]:
    # Ретраи для rate limit (МойСклад 429) и временных сетевых ошибок
    backoffs = [0.5, 1, 2, 4, 8]

    last_exc: Exception | None = None
    for attempt, sleep_s in enumerate([0.0] + backoffs):
        if sleep_s:
            time.sleep(sleep_s)

        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=(10, timeout),
            )
        except requests.Timeout as e:
            last_exc = HttpError(f"TIMEOUT {method} {url}")
            continue
        except requests.RequestException as e:
            last_exc = e
            continue

        text = resp.text or ""

        # retry на rate-limit
        if resp.status_code == 429:
            last_exc = HttpError(f"429 {url} -> {text[:300]}")
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
