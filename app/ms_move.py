from __future__ import annotations

from typing import Any, Dict, List, Optional

from .moysklad import MoySkladClient
from .http import HttpError

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
FBO_EXT_PREFIX = "OZON_FBO:"


def fbo_external_code(order_number: str) -> str:
    return f"{FBO_EXT_PREFIX}{order_number}"


def _pick_latest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows_sorted = sorted(rows, key=lambda r: (r.get("updated") or ""), reverse=True)
    return rows_sorted[0]


def find_moves_by_external(ms: MoySkladClient, external_code: str, limit: int = 100) -> List[Dict[str, Any]]:
    res = ms.get("/entity/move", params={"filter": f"externalCode={external_code}", "limit": limit})
    return res.get("rows") or []


def dedup_moves_by_external(ms: MoySkladClient, external_code: str, dry_run: bool) -> Optional[Dict[str, Any]]:
    rows = find_moves_by_external(ms, external_code)
    if not rows:
        return None

    keep = _pick_latest(rows)
    dups = [r for r in rows if r.get("id") and r["id"] != keep.get("id")]

    for d in dups:
        if dry_run:
            print({"action": "dry_run_delete_duplicate_move", "id": d["id"], "externalCode": external_code})
        else:
            ms.delete(f"/entity/move/{d['id']}")
            print({"action": "deleted_duplicate_move", "id": d["id"], "externalCode": external_code})

    return keep


def build_move_positions_from_order_positions(order_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    res: List[Dict[str, Any]] = []
    for p in order_positions or []:
        ass_meta = (p.get("assortment") or {}).get("meta")
        if not ass_meta:
            continue
        qty = float(p.get("quantity") or 0)
        if qty <= 0:
            continue
        row = {"assortment": {"meta": ass_meta}, "quantity": qty}
        # price в move можно не ставить, но если есть — оставим
        if p.get("price") is not None:
            row["price"] = int(p.get("price") or 0)
        res.append(row)
    return res


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
