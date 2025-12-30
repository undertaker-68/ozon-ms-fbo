from __future__ import annotations

from typing import Any, Dict, List, Optional

from .moysklad import MoySkladClient

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


def find_demand_by_name(ms: MoySkladClient, name: str) -> Optional[Dict[str, Any]]:
    res = ms.get("/entity/demand", params={"filter": f"name={name}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None


def find_demands_by_external(ms: MoySkladClient, external_code: str, limit: int = 100) -> List[Dict[str, Any]]:
    res = ms.get("/entity/demand", params={"filter": f"externalCode={external_code}", "limit": limit})
    return (res.get("rows") or [])


def _pick_latest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows_sorted = sorted(rows, key=lambda r: (r.get("updated") or ""), reverse=True)
    return rows_sorted[0]


def dedup_demands_by_external(ms: MoySkladClient, external_code: str, dry_run: bool) -> Optional[Dict[str, Any]]:
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
    # В МС demand positions структура такая же: assortment + quantity + price
    res: List[Dict[str, Any]] = []
    for p in order_positions or []:
        ass = (p.get("assortment") or {}).get("meta")
        if not ass:
            continue
        res.append(
            {
                "assortment": {"meta": ass},
                "quantity": float(p.get("quantity") or 0),
                "price": int(p.get("price") or 0),
            }
        )
    return res


def create_demand(ms: MoySkladClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    return ms.post("/entity/demand", payload)


def try_apply_demand(ms: MoySkladClient, demand_id: str) -> bool:
    try:
        ms.put(f"/entity/demand/{demand_id}", {"applicable": True})
        return True
    except Exception:
        return False
