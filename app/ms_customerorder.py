from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

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
    store_id: Optional[str] = None  # склад (если надо)


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
    if d.store_id:
        payload["store"] = _ms_ref("store", d.store_id)
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


def ensure_customerorder(ms: MoySkladClient, payload: Dict[str, Any], dry_run: bool) -> Dict[str, Any]:
    """
    Upsert по externalCode:
    - если есть дубликаты -> удаляем лишние
    - если нет -> создаем
    - если есть -> обновляем patch’ем
    """
    ext = payload.get("externalCode") or ""
    if not ext:
        raise ValueError("customerorder payload must include externalCode")

    existing = dedup_customerorders_by_external(ms, ext, dry_run=dry_run)

    if not existing:
        if dry_run:
            print({"action": "dry_run_create_customerorder", "name": payload.get("name"), "externalCode": ext})
            return {"id": "dry_run", "name": payload.get("name"), "externalCode": ext}
        created = create_customerorder(ms, payload)
        print({"action": "created_customerorder", "id": created.get("id"), "name": created.get("name")})
        return created

    # PATCH: обновляем только то, что реально нужно
    patch: Dict[str, Any] = {}
    for k in ("description", "deliveryPlannedMoment", "positions", "store", "state"):
        if k in payload:
            patch[k] = payload[k]

    if dry_run:
        print({"action": "dry_run_update_customerorder", "id": existing.get("id"), "name": existing.get("name")})
        return {"id": existing.get("id"), "name": existing.get("name"), "updated": True}

    updated = ms.put(f"/entity/customerorder/{existing['id']}", patch)
    print({"action": "updated_customerorder", "id": updated.get("id"), "name": updated.get("name"), "updated": True})
    return updated
