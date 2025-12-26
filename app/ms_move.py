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


def _assortment_meta_from_order_pos(p: Dict[str, Any]) -> Dict[str, Any]:
    ass = p.get("assortment") or {}
    meta = ass.get("meta")
    if meta:
        return {"meta": meta}
    # иногда уже может быть {"meta": {...}}
    if "meta" in ass:
        return {"meta": ass["meta"]}
    return {"meta": {}}


def find_moves_by_external(ms, external_code: str, limit: int = 100) -> List[dict]:
    res = ms.get("/entity/move", params={"filter": f"externalCode={external_code}", "limit": limit})
    return res.get("rows") or []


def _pick_latest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows_sorted = sorted(rows, key=lambda r: (r.get("updated") or ""), reverse=True)
    return rows_sorted[0]


def dedup_moves_by_external(ms, external_code: str, dry_run: bool) -> Optional[dict]:
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
                "assortment": _assortment_meta_from_order_pos(p),
                "quantity": qty,
                "price": int(price),  # важно: цена как в заказе
            }
        )
    return out


def create_move(
    ms,
    *,
    name: str,
    external_code: str,
    organization_id: str,
    state_id: str,
    source_store_id: str,
    target_store_id: str,
    description: str,
    customerorder_id: str,
    positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": _ms_ref("organization", organization_id),
        "state": _ms_ref("state", state_id),
        "sourceStore": _ms_ref("store", source_store_id),
        "targetStore": _ms_ref("store", target_store_id),
        "description": description,
        "customerOrder": _ms_ref("customerorder", customerorder_id),
        "positions": positions,
        "applicable": False,
    }
    return ms.post("/entity/move", payload)


def update_move_positions_only(ms, move_id: str, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    return ms.put(f"/entity/move/{move_id}", {"positions": positions})


def try_apply_move(ms, move_id: str) -> Dict[str, Any]:
    try:
        updated = ms.put(f"/entity/move/{move_id}", {"applicable": True})
        return {"action": "move_applied", "id": move_id, "updated": updated}
    except HttpError as e:
        txt = str(e)
        if "Нельзя переместить товар" in txt:
            return {"action": "move_left_unapplied", "id": move_id, "reason": "not_enough_stock"}
        raise


def link_move_to_customerorder(ms, order_id: str, move_id: str) -> Dict[str, Any]:
    # UI-связка может не отображаться как "documents", но поле связи в move/customerOrder — главное.
    return {"action": "order_linked_to_move", "order_id": order_id, "move_id": move_id}
