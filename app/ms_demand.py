from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.http import HttpError

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


def find_demands_by_external(ms, external_code: str, limit: int = 100) -> List[dict]:
    res = ms.get("/entity/demand", params={"filter": f"externalCode={external_code}", "limit": limit})
    return res.get("rows") or []


def _pick_latest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows_sorted = sorted(rows, key=lambda r: (r.get("updated") or ""), reverse=True)
    return rows_sorted[0]


def dedup_demands_by_external(ms, external_code: str, dry_run: bool) -> Optional[dict]:
    rows = find_demands_by_external(ms, external_code)
    if not rows:
        return None
    keep = _pick_latest(rows)
    dups = [r for r in rows if r.get("id") and r["id"] != keep.get("id")]

    for d in dups:
        if dry_run:
            print({"action": "dry_run_delete_duplicate_demand", "id": d["id"], "externalCode": external_code})
        else:
            ms.delete(f"/entity/demand/{d['id']}")
            print({"action": "deleted_duplicate_demand", "id": d["id"], "externalCode": external_code})

    return keep


def build_demand_positions_from_order_positions(order_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in order_positions:
        qty = float(p.get("quantity") or 0)
        if qty <= 0:
            continue
        ass = p.get("assortment") or {}
        meta = ass.get("meta") or ass.get("meta", {})
        out.append({"assortment": {"meta": meta}, "quantity": qty, "price": int(p.get("price") or 0)})
    return out


def create_demand(
    ms,
    *,
    name: str,
    external_code: str,
    organization_id: str,
    agent_id: str,
    state_id: str,
    store_id: str,
    description: str,
    customerorder_id: str,
    positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": _ms_ref("organization", organization_id),
        "agent": _ms_ref("counterparty", agent_id),
        "state": _ms_ref("state", state_id),
        "store": _ms_ref("store", store_id),
        "description": description,
        "customerOrder": _ms_ref("customerorder", customerorder_id),
        "positions": positions,
        "applicable": False,
    }
    return ms.post("/entity/demand", payload)


def try_apply_demand(ms, demand_id: str) -> Dict[str, Any]:
    try:
        updated = ms.put(f"/entity/demand/{demand_id}", {"applicable": True})
        return {"action": "demand_applied", "id": demand_id, "updated": updated}
    except HttpError as e:
        return {"action": "demand_left_unapplied", "id": demand_id, "error": str(e)}
