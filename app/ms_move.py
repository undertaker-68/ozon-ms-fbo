from __future__ import annotations

from typing import Any, Dict, Optional

from .http import HttpError
from .moysklad import MoySkladClient


def _find_moves_by_external(ms: MoySkladClient, external: str) -> list[Dict[str, Any]]:
    res = ms.get("/entity/move", params={"filter": f"externalCode={external}", "limit": 100})
    return res.get("rows") or []


def dedup_moves_by_external(ms: MoySkladClient, external: str, *, dry_run: bool) -> Optional[Dict[str, Any]]:
    rows = _find_moves_by_external(ms, external)
    if not rows:
        return None

    rows_sorted = sorted(rows, key=lambda r: (r.get("moment") or "", r.get("created") or "", r.get("id") or ""))
    keep = rows_sorted[0]
    dups = rows_sorted[1:]

    for d in dups:
        if dry_run:
            print({"action": "dry_run_delete_move_duplicate", "id": d["id"], "externalCode": external})
        else:
            ms.delete(f"/entity/move/{d['id']}")
            print({"action": "deleted_move_duplicate", "id": d["id"], "externalCode": external})

    return keep


def build_move_positions_from_order_positions(order_positions: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    positions = []
    for p in order_positions:
        ass = p.get("assortment") or {}
        # у нас в order_positions: {"assortment":{"meta":...}}
        meta = ass.get("meta") if "meta" in ass else ass
        positions.append(
            {
                "assortment": {"meta": meta},
                "quantity": float(p.get("quantity") or 0),
                "price": int(p.get("price") or 0),
            }
        )
    return positions


def create_move(
    ms: MoySkladClient,
    *,
    name: str,
    external_code: str,
    organization_id: str,
    state_id: str,
    source_store_id: str,
    target_store_id: str,
    description: str,
    customerorder_id: str,
    positions: list[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = {
        "name": name,
        "externalCode": external_code,
        "organization": ms.meta("organization", organization_id),
        "state": ms.meta("state", state_id),
        "sourceStore": ms.meta("store", source_store_id),
        "targetStore": ms.meta("store", target_store_id),
        "description": description,
        "customerOrder": ms.meta("customerorder", customerorder_id),
        "positions": positions,
        "applicable": False,
    }
    return ms.post("/entity/move", payload)


def update_move_positions_only(ms: MoySkladClient, move_id: str, positions: list[Dict[str, Any]]) -> None:
    ms.put(f"/entity/move/{move_id}", {"positions": positions})


def try_apply_move(ms: MoySkladClient, move_id: str) -> Dict[str, Any]:
    try:
        ms.put(f"/entity/move/{move_id}", {"applicable": True})
        return {"action": "move_applied", "id": move_id}
    except HttpError as e:
        msg = str(e)
        if "Нельзя переместить товар" in msg or "code\" : 3007" in msg:
            return {"action": "move_left_unapplied", "id": move_id, "reason": "not_enough_stock"}
        return {"action": "move_apply_failed", "id": move_id, "error": msg[:300]}
