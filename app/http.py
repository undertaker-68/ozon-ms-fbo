from __future__ import annotations

import json
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
    try:
        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )
    except requests.RequestException as e:
        raise HttpError(f"HTTP request failed: {e}") from e

    text = resp.text or ""
    if resp.status_code >= 400:
        raise HttpError(f"{resp.status_code} {url} -> {text[:1500]}")

    if not text.strip():
        return {}

    try:
        return resp.json()
    except json.JSONDecodeError as e:
        raise HttpError(f"Invalid JSON from {url}: {text[:1500]}") from e
