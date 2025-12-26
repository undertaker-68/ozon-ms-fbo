from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, List

from .moysklad import MoySkladClient


MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
FBO_EXT_PREFIX = "OZON_FBO:"


def fbo_external_code(order_number: str) -> str:
    return f"{FBO_EXT_PREFIX}{order_number}"


def _ms_ref(entity: str, id_: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"{MS_BASE}/entity/{entity}/{id_}",
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


def build_customerorder_payload(d: CustomerOrderDraft, positions: list[dict]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": d.name,
        "externalCode": fbo_external_code(d.name),
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


def find_customerorders_by_external(ms: MoySkladClient, external_code: str, limit: int = 100) -> List[Dict[str, Any]]:
    res = ms.get("/entity/customerorder", params={"filter": f"externalCode={external_code}", "limit": limit})
    return (res.get("rows") or [])


def _pick_latest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # updated у МС строка "YYYY-MM-DD HH:MM:SS.mmm" — лексикографически сортируется корректно
    rows_sorted = sorted(rows, key=lambda r: (r.get("updated") or ""), reverse=True)
    return rows_sorted[0]


def dedup_customerorders_by_external(ms: MoySkladClient, external_code: str, dry_run: bool) -> Optional[Dict[str, Any]]:
    rows = find_customerorders_by_external(ms, external_code)
    if not rows:
        return None
    keep = _pick_latest(rows)
    dups = [r for r in rows if r.get("id") and r["id"] != keep.get("id")]

    for d in dups:
        if dry_run:
            print({"action": "dry_run_delete_duplicate_customerorder", "id": d["id"], "externalCode": external_code})
        else:
            ms.delete(f"/entity/customerorder/{d['id']}")
            print({"action": "deleted_duplicate_customerorder", "id": d["id"], "externalCode": external_code})

    return keep


def ensure_customerorder(ms: MoySkladClient, payload: dict, dry_run: bool = False) -> dict:
    """
    Создаём/обновляем CustomerOrder.
    Ключ: externalCode = OZON_FBO:<order_number>
    Обновляем только: deliveryPlannedMoment, positions, description, store (если есть).
    """
    name = payload["name"]
    ext = payload.get("externalCode") or fbo_external_code(name)
    payload["externalCode"] = ext

    existing = dedup_customerorders_by_external(ms, ext, dry_run=dry_run)

    patch: Dict[str, Any] = {}
    if "deliveryPlannedMoment" in payload:
        patch["deliveryPlannedMoment"] = payload["deliveryPlannedMoment"]
    if "positions" in payload:
        patch["positions"] = payload["positions"]
    if "description" in payload:
        patch["description"] = payload["description"]
    if "store" in payload and isinstance(payload["store"], dict):
        patch["store"] = payload["store"]

    if not existing:
        if dry_run:
            return {"action": "dry_run_create", "name": name, "payload": payload}
        created = create_customerorder(ms, payload)
        return {"action": "created", "id": created.get("id"), "name": name}

    if dry_run:
        return {"action": "dry_run_update", "id": existing["id"], "name": name, "patch": patch}

    ms.put(f"/entity/customerorder/{existing['id']}", patch)
    return {"action": "updated", "id": existing["id"], "name": name, "updated": True}
