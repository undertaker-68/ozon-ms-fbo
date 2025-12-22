from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from .moysklad import MoySkladClient


def _ms_ref(entity: str, id_: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/{id_}",
            "type": entity,
            "mediaType": "application/json",
        }
    }


@dataclass(frozen=True)
class CustomerOrderDraft:
    name: str
    organization_id: str
    agent_id: str
    state_id: str
    saleschannel_id: str
    shipment_planned_at: Optional[datetime]
    description: str  # комментарий


def build_customerorder_payload(d: CustomerOrderDraft) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": d.name,
        "organization": _ms_ref("organization", d.organization_id),
        "agent": _ms_ref("counterparty", d.agent_id),
        "state": _ms_ref("state", d.state_id),
        "salesChannel": _ms_ref("saleschannel", d.saleschannel_id),
        "description": d.description,
    }
    if d.shipment_planned_at:
        # МойСклад принимает ISO-дату/время
        payload["shipmentPlanned"] = d.shipment_planned_at.isoformat()
    return payload


def create_customerorder(ms: MoySkladClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    return ms.post("/entity/customerorder", payload)


def find_customerorder_by_name(ms: MoySkladClient, name: str) -> Optional[Dict[str, Any]]:
    # МС позволяет искать через filter= по полям, для name: filter=name=<...>
    # Берём первый найденный
    res = ms.get("/entity/customerorder", params={"filter": f"name={name}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None
