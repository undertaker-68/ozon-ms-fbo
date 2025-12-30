from __future__ import annotations

from typing import Any, Dict, List, Optional

from .moysklad import MoySkladClient
from .http import HttpError


def _find_moves_by_external(ms: MoySkladClient, external_code: str) -> List[Dict[str, Any]]:
    res = ms.get("/entity/move", params={"filter": f"externalCode={external_code}", "limit": 100})
    return res.get("rows") or []


# ПУБЛИЧНЫЙ алиас (чтобы импорты не падали)
def find_moves_by_external(ms: MoySkladClient, external_code: str) -> List[Dict[str, Any]]:
    return _find_moves_by_external(ms, external_code)


def dedup_moves_by_external(ms: MoySkladClient, external_code: str, dry_run: bool) -> Optional[Dict[str, Any]]:
    rows = _find_moves_by_external(ms, external_code)
    if not rows:
        return None

    rows_sorted = sorted(rows, key=lambda r: r.get("updated") or "")
    keep = rows_sorted[-1]

    extras = [r for r in rows_sorted if r.get("id") != keep.get("id")]
    if extras:
        if dry_run:
            return keep
        for r in extras:
            rid = r.get("id")
            if rid:
                ms.delete(f"/entity/move/{rid}")

    return keep


def create_move(ms: MoySkladClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    return ms.post("/entity/move", payload)


def update_move_positions_only(ms: MoySkladClient, move_id: str, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    return ms.put(f"/entity/move/{move_id}", {"positions": positions})


# совместимость с твоими импортами
def update_move_positions_only_(ms: MoySkladClient, move_id: str, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    return update_move_positions_only(ms, move_id, positions)


def try_apply_move(ms: MoySkladClient, move_id: str) -> Dict[str, Any]:
    try:
        ms.put(f"/entity/move/{move_id}", {"applicable": True})
        return {"action": "move_applied", "id": move_id}
    except HttpError as e:
        msg = str(e)
        # “Нельзя переместить товар, которого нет на складе”
        if "Нельзя переместить товар" in msg or "code\" : 3007" in msg:
            return {"action": "move_left_unapplied", "id": move_id, "reason": "not_enough_stock"}
        return {"action": "move_apply_failed", "id": move_id, "error": msg[:300]}


def build_move_positions_from_order_positions(order_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in order_positions:
        ass = p.get("assortment") or {}
        qty = p.get("quantity")
        price = p.get("price", 0)
        if not ass or qty in (None, 0, 0.0):
            continue
        out.append({"assortment": ass, "quantity": qty, "price": price})
    return out
