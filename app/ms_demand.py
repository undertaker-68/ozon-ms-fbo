from __future__ import annotations

from typing import Any, Dict, List, Optional

from .moysklad import MoySkladClient
from .http import HttpError


def _find_demands_by_external(ms: MoySkladClient, external_code: str) -> List[Dict[str, Any]]:
    res = ms.get("/entity/demand", params={"filter": f"externalCode={external_code}", "limit": 100})
    return res.get("rows") or []


def find_demands_by_external(ms: MoySkladClient, external_code: str) -> List[Dict[str, Any]]:
    return _find_demands_by_external(ms, external_code)


def dedup_demands_by_external(ms: MoySkladClient, external_code: str, dry_run: bool) -> Optional[Dict[str, Any]]:
    rows = _find_demands_by_external(ms, external_code)
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
                ms.delete(f"/entity/demand/{rid}")

    return keep


def create_demand(ms: MoySkladClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    return ms.post("/entity/demand", payload)


def update_demand_positions_only(ms: MoySkladClient, demand_id: str, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    return ms.put(f"/entity/demand/{demand_id}", {"positions": positions})


def try_apply_demand(ms: MoySkladClient, demand_id: str) -> Dict[str, Any]:
    try:
        ms.put(f"/entity/demand/{demand_id}", {"applicable": True})
        return {"action": "demand_applied", "id": demand_id}
    except HttpError as e:
        return {"action": "demand_left_unapplied", "id": demand_id, "error": str(e)[:300]}


def build_demand_positions_from_order_positions(order_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in order_positions:
        ass = p.get("assortment") or {}
        qty = p.get("quantity")
        price = p.get("price", 0)
        if not ass or qty in (None, 0, 0.0):
            continue
        out.append({"assortment": ass, "quantity": qty, "price": price})
    return out
