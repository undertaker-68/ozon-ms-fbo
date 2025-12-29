from __future__ import annotations

from typing import Any, Dict, Optional

from .moysklad import MoySkladClient


def fbo_external_code(order_number: str) -> str:
    return f"OZON_FBO:{order_number}"


def _find_orders_by_external(ms: MoySkladClient, external: str) -> list[Dict[str, Any]]:
    res = ms.get("/entity/customerorder", params={"filter": f"externalCode={external}", "limit": 100})
    return res.get("rows") or []


def dedup_customerorders_by_external(
    ms: MoySkladClient,
    external: str,
    *,
    dry_run: bool,
) -> Optional[Dict[str, Any]]:
    rows = _find_orders_by_external(ms, external)
    if not rows:
        return None

    # оставляем самый ранний (по moment/created)
    rows_sorted = sorted(rows, key=lambda r: (r.get("moment") or "", r.get("created") or "", r.get("id") or ""))
    keep = rows_sorted[0]
    dups = rows_sorted[1:]

    for d in dups:
        if dry_run:
            print({"action": "dry_run_delete_customerorder_duplicate", "id": d["id"], "externalCode": external})
        else:
            ms.delete(f"/entity/customerorder/{d['id']}")
            print({"action": "deleted_customerorder_duplicate", "id": d["id"], "externalCode": external})

    return keep


def ensure_customerorder(ms: MoySkladClient, payload: Dict[str, Any], *, dry_run: bool) -> Dict[str, Any]:
    """
    Создаём или обновляем customerorder по externalCode.
    Обновляем только: deliveryPlannedMoment, positions, description, store.
    """
    name = payload["name"]
    external = payload["externalCode"]

    existing = dedup_customerorders_by_external(ms, external, dry_run=dry_run)

    if not existing:
        if dry_run:
            return {"action": "dry_run_create", "name": name, "payload": payload}
        created = ms.post("/entity/customerorder", payload)
        return {"action": "created", "id": created.get("id"), "name": name}

    patch = {
        "deliveryPlannedMoment": payload.get("deliveryPlannedMoment"),
        "positions": payload.get("positions") or [],
        "description": payload.get("description"),
        "store": payload.get("store"),
    }

    if dry_run:
        return {"action": "dry_run_update", "id": existing["id"], "name": name, "patch": patch}

    ms.put(f"/entity/customerorder/{existing['id']}", patch)
    return {"action": "updated", "id": existing["id"], "name": name, "updated": True}
