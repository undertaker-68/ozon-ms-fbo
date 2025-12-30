from __future__ import annotations

from typing import Any, Dict, Optional

from .http import HttpError
from .moysklad import MoySkladClient


def find_demands_by_external(ms: MoySkladClient, external: str, limit: int = 100) -> list[Dict[str, Any]]:
    res = ms.get("/entity/demand", params={"filter": f"externalCode={external}", "limit": limit})
    return res.get("rows") or []


def dedup_demands_by_external(ms: MoySkladClient, external: str, *, dry_run: bool) -> Optional[Dict[str, Any]]:
    rows = find_demands_by_external(ms, external)
    if not rows:
        return None

    # оставляем самую свежую
    rows_sorted = sorted(rows, key=lambda r: (r.get("updated") or "", r.get("moment") or "", r.get("id") or ""), reverse=True)
    keep = rows_sorted[0]
    dups = [r for r in rows_sorted[1:] if r.get("id")]

    for d in dups:
        if dry_run:
            print({"action": "dry_run_delete_demand_duplicate", "id": d["id"], "externalCode": external})
        else:
            ms.delete(f"/entity/demand/{d['id']}")
            print({"action": "deleted_demand_duplicate", "id": d["id"], "externalCode": external})

    return keep


def build_demand_positions_from_order_positions(order_positions: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for p in order_positions:
        qty = float(p.get("quantity") or 0)
        if qty <= 0:
            continue

        ass = p.get("assortment") or {}
        meta = ass.get("meta") if isinstance(ass, dict) and "meta" in ass else ass

        out.append(
            {
                "assortment": {"meta": meta},
                "quantity": qty,
                "price": int(p.get("price") or 0),
            }
        )
    return out


def create_demand(
    ms: MoySkladClient,
    *,
    name: str,
    external_code: str,
    organization_id: str,
    agent_id: str,
    state_id: str,
    store_id: str,
    description: str,
    customerorder_id: str,
    positions: list[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = {
        "name": name,
        "externalCode": external_code,
        "organization": ms.meta("organization", organization_id),
        "agent": ms.meta("counterparty", agent_id),
        "state": ms.meta("state", state_id),
        "store": ms.meta("store", store_id),
        "description": description,
        "customerOrder": ms.meta("customerorder", customerorder_id),
        "positions": positions,
        "applicable": False,
    }
    return ms.post("/entity/demand", payload)


def try_apply_demand(ms: MoySkladClient, demand_id: str) -> Dict[str, Any]:
    try:
        ms.put(f"/entity/demand/{demand_id}", {"applicable": True})
        return {"action": "demand_applied", "id": demand_id}
    except HttpError as e:
        msg = str(e)
        return {"action": "demand_apply_failed", "id": demand_id, "error": msg[:400]}
