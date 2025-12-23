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


def build_customerorder_payload(
    d: CustomerOrderDraft,
    positions: list[dict],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": d.name,
        "organization": _ms_ref("organization", d.organization_id),
        "agent": _ms_ref("counterparty", d.agent_id),
        "state": _ms_ref("state", d.state_id),
        "salesChannel": _ms_ref("saleschannel", d.saleschannel_id),
        "description": d.description,
        "positions": positions,
    }
    if d.shipment_planned_at:
        payload["deliveryPlannedMoment"] = d.shipment_planned_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return payload

def create_customerorder(ms: MoySkladClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    return ms.post("/entity/customerorder", payload)


def find_customerorder_by_name(ms: MoySkladClient, name: str) -> Optional[Dict[str, Any]]:
    # МС позволяет искать через filter= по полям, для name: filter=name=<...>
    # Берём первый найденный
    res = ms.get("/entity/customerorder", params={"filter": f"name={name}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None

def update_customerorder(
    ms: MoySkladClient,
    order_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    # name обновлять не нужно, он ключ
    upd = payload.copy()
    upd.pop("name", None)
    return ms.put(f"/entity/customerorder/{order_id}", upd)

def ensure_customerorder(
    ms: MoySkladClient,
    payload: Dict[str, Any],
    *,
    dry_run: bool,
) -> Dict[str, Any]:
    """
    Upsert логика:
    - если заказ с таким name есть → update
    - если нет → create
    """
    name = payload.get("name")
    if not name:
        raise ValueError("CustomerOrder payload must contain 'name'")

    existing = find_customerorder_by_name(ms, name)

    if existing:
        order_id = existing.get("id")
        if dry_run:
            return {
                "action": "dry_run_update",
                "id": order_id,
                "name": name,
                "payload": payload,
            }

        updated = update_customerorder(ms, order_id, payload)
        return {
            "action": "updated",
            "id": updated.get("id"),
            "name": updated.get("name"),
        }

    # заказа нет → создаём
    if dry_run:
        return {
            "action": "dry_run_create",
            "name": name,
            "payload": payload,
        }

    created = create_customerorder(ms, payload)
    return {
        "action": "created",
        "id": created.get("id"),
        "name": created.get("name"),
    }

