from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.http import HttpError


def _ms_ref(entity: str, id_: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/{id_}",
            "type": entity,
            "mediaType": "application/json",
        }
    }


def find_demand_by_name(ms, name: str) -> Optional[dict]:
    res = ms.get("/entity/demand", params={"filter": f"name={name}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None


def find_demands_by_external(ms, external_code: str) -> List[dict]:
    res = ms.get("/entity/demand", params={"filter": f"externalCode={external_code}", "limit": 100})
    return res.get("rows") or []


def dedup_demands_by_external(ms, external_code: str, *, dry_run: bool) -> Optional[dict]:
    rows = find_demands_by_external(ms, external_code)
    if not rows:
        return None
    # оставляем самый свежий (по updated)
    rows_sorted = sorted(rows, key=lambda r: r.get("updated") or "", reverse=True)
    keep = rows_sorted[0]
    dups = rows_sorted[1:]

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
        price = p.get("price")
        if price is None:
            price = 0
        out.append(
            {
                "assortment": p["assortment"],  # ожидаем {"meta": ...}
                "quantity": qty,
                "price": int(price),
            }
        )
    return out


def create_demand(
    ms,
    *,
    name: str,
    external_code: str,
    organization_id: str,
    agent_id: str,
    store_id: str,
    state_id: str,
    description: str,
    customerorder_id: str,
    positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": _ms_ref("organization", organization_id),
        "agent": _ms_ref("counterparty", agent_id),
        "store": _ms_ref("store", store_id),
        "state": _ms_ref("state", state_id),
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
