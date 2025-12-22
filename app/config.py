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


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class OzonCabinet:
    name: str
    client_id: str
    api_key: str
    ms_saleschannel_id: str


@dataclass(frozen=True)
class Config:
    cabinets: list[OzonCabinet]

    moysklad_token: str
    ms_organization_id: str
    ms_state_fbo_id: str
    ms_agent_id: str

    fbo_planned_from: date | None
    fbo_dry_run: bool


def load_config() -> Config:
    load_dotenv()

    planned_from_raw = os.getenv("FBO_PLANNED_FROM")
    planned_from = date.fromisoformat(planned_from_raw) if planned_from_raw else None

    cabinets = [
        OzonCabinet(
            name="cab1",
            client_id=_env("OZON1_CLIENT_ID"),
            api_key=_env("OZON1_API_KEY"),
            ms_saleschannel_id=_env("MS_SALESCHANNEL_ID_CAB1"),
        ),
        OzonCabinet(
            name="cab2",
            client_id=_env("OZON2_CLIENT_ID"),
            api_key=_env("OZON2_API_KEY"),
            ms_saleschannel_id=_env("MS_SALESCHANNEL_ID_CAB2"),
        ),
    ]

    return Config(
        cabinets=cabinets,
        moysklad_token=_env("MOYSKLAD_TOKEN"),
        ms_organization_id=_env("MS_ORGANIZATION_ID"),
        ms_state_fbo_id=_env("MS_STATE_FBO_ID"),
        ms_agent_id=_env("MS_AGENT_ID"),
        fbo_planned_from=planned_from,
        fbo_dry_run=_env_bool("FBO_DRY_RUN", default=True),
    )
