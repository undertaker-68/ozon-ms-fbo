from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

from dotenv import load_dotenv


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise ValueError(f"Missing env var: {name}")
    return v


def _env_int(name: str, default: int | None = None) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise ValueError(f"Missing env var: {name}")
        return default
    return int(v)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Config:
    ozon_client_id: str
    ozon_api_key: str

    moysklad_token: str | None

    fbo_planned_from: date | None
    fbo_dry_run: bool


def load_config() -> Config:
    load_dotenv()

    planned_from_raw = os.getenv("FBO_PLANNED_FROM")
    planned_from = None
    if planned_from_raw:
        planned_from = date.fromisoformat(planned_from_raw)

    ms_token = os.getenv("MOYSKLAD_TOKEN") or None

    return Config(
        ozon_client_id=_env("OZON_CLIENT_ID"),
        ozon_api_key=_env("OZON_API_KEY"),
        moysklad_token=ms_token,
        fbo_planned_from=planned_from,
        fbo_dry_run=_env_bool("FBO_DRY_RUN", default=True),
    )
