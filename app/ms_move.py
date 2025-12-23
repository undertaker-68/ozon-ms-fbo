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
    # для state ссылка на metadata/states
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/metadata/states/{state_id}",
            "type": "state",
            "mediaType": "application/json",
            "metadataHref": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/metadata",
        }
    }


def find_move_by_name(ms, name: str) -> Optional[dict]:
    res = ms.get("/entity/move", params={"filter": f"name={name}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None


def build_move_positions_from_order_positions(order_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Перемещение: берём только assortment+quantity.
    (price для move не нужен)
    """
    out: List[Dict[str, Any]] = []
    for p in order_positions:
        ass = p.get("assortment")
        qty = p.get("quantity")
        if not ass or qty is None:
            continue
        out.append({"assortment": ass, "quantity": qty})
    return out


def create_move(
    ms,
    *,
    name: str,
    description: str,
    organization_id: str,
    source_store_id: str,
    target_store_id: str,
    state_id: str,
    positions: List[Dict[str, Any]],
) -> dict:
    payload: Dict[str, Any] = {
        "name": name,
        "description": description,
        "organization": _ms_ref("organization", organization_id),
        "sourceStore": _ms_ref("store", source_store_id),
        "targetStore": _ms_ref("store", target_store_id),
        "state": _ms_state_ref("move", state_id),
        "positions": positions,
        "applicable": False,  # всегда создаём непроведенным
    }
    return ms.post("/entity/move", payload)


def update_move_positions_only(ms, move_id: str, *, positions: List[Dict[str, Any]], description: str) -> dict:
    # обновляем только состав и комментарий
    patch = {
        "positions": positions,
        "description": description,
    }
    return ms.put(f"/entity/move/{move_id}", patch)


def try_apply_move(ms, move_id: str) -> Dict[str, Any]:
    """
    Пытаемся провести: applicable=True.
    Если ошибка именно про отсутствие товара на складе — оставляем непроведенным.
    """
    try:
        updated = ms.put(f"/entity/move/{move_id}", {"applicable": True})
        return {"applied": True, "move": updated}
    except HttpError as e:
        msg = str(e)
        if "Нельзя переместить товар" in msg and "нет на складе" in msg:
            return {"applied": False, "reason": "not_enough_stock"}
        # любая другая ошибка — пробрасываем
        raise
