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


def _ms_state_ref(entity: str, state_id: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/metadata/states/{state_id}",
            "type": "state",
            "mediaType": "application/json",
            "metadataHref": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/metadata",
        }
    }


def find_demand_by_name(ms, name: str) -> Optional[dict]:
    res = ms.get("/entity/demand", params={"filter": f"name={name}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None


def build_demand_positions_from_order_positions(order_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # demand умеет price, берём как в заказе
    out: List[Dict[str, Any]] = []
    for p in order_positions:
        ass = p.get("assortment")
        qty = p.get("quantity")
        price = p.get("price")
        if not ass or qty is None:
            continue
        row = {"assortment": ass, "quantity": qty}
        if price is not None:
            row["price"] = price
        out.append(row)
    return out


def create_demand(
    ms,
    *,
    name: str,
    description: str,
    organization_id: str,
    agent_id: str,
    store_id: str,
    state_id: str,
    customerorder_id: str,
    positions: List[Dict[str, Any]],
) -> dict:
    payload: Dict[str, Any] = {
        "name": name,
        "description": description,
        "organization": _ms_ref("organization", organization_id),
        "agent": _ms_ref("counterparty", agent_id),
        "store": _ms_ref("store", store_id),
        "state": _ms_state_ref("demand", state_id),
        "customerOrder": _ms_ref("customerorder", customerorder_id),
        "positions": positions,
        "applicable": False,  # создаём непроведённой, потом пытаемся провести
    }
    return ms.post("/entity/demand", payload)


def try_apply_demand(ms, demand_id: str) -> Dict[str, Any]:
    try:
        updated = ms.put(f"/entity/demand/{demand_id}", {"applicable": True})
        return {"applied": True, "demand": updated}
    except HttpError as e:
        msg = str(e)
        # универсально ловим нехватку остатков
        if ("нет на складе" in msg) or ("Недостаточно" in msg) or ("остат" in msg.lower()):
            return {"applied": False, "reason": "not_enough_stock"}
        raise
