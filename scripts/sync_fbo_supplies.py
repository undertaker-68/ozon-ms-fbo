from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import (
    CustomerOrderDraft,
    build_customerorder_payload,
    ensure_customerorder,
    fbo_external_code,
    find_customerorders_by_external,
)

from app.ms_move import (
    dedup_moves_by_external,
    create_move,
    update_move_positions_only,
    build_move_positions_from_order_positions,
    try_apply_move,
)

from app.ms_demand import (
    dedup_demands_by_external,
    create_demand,
    build_demand_positions_from_order_positions,
    try_apply_demand,
)

# =========================
# OZON STATES (numeric)
# =========================
READY_TO_SUPPLY = 2
ACCEPTED_AT_SUPPLY_WAREHOUSE = 3
IN_TRANSIT = 4
ACCEPTANCE_AT_STORAGE_WAREHOUSE = 5
COMPLETED = 8

# участвуют в синхронизации
SYNC_STATES = (
    READY_TO_SUPPLY,
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
)

# по этим статусам создаём отгрузку
DEMAND_OZON_STATES = {
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
}

# =========================
# MOYSKLAD CONSTANTS
# (берём из cfg, но оставлю fallback если пусто)
# =========================
MOVE_SOURCE_STORE_ID = "7cdb9b20-9910-11ec-0a80-08670002d998"
MOVE_TARGET_STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"  # FBO склад назначение


def _parse_timeslot_from(detail: Dict[str, Any]) -> datetime:
    """
    detail["timeslot"]["timeslot"]["from"] -> ISO Z
    """
    ts = (((detail.get("timeslot") or {}).get("timeslot") or {}).get("from")) or ""
    # пример: "2025-12-25T11:00:00.000Z"
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _ms_meta_from_assortment_dict(a: Dict[str, Any]) -> Dict[str, Any]:
    """
    В components приходит assortment без вложенного meta:
    {"href": ".../entity/product/...", "type":"product", "mediaType":"application/json", ...}
    Приводим к {"meta": {...}}
    """
    if "meta" in a:
        return a["meta"]
    return {
        "href": a.get("href"),
        "type": a.get("type"),
        "mediaType": a.get("mediaType") or "application/json",
    }


def _expand_ms_article_to_positions(ms: MoySkladClient, article: str, qty: float) -> List[Dict[str, Any]]:
    """
    Возвращает позиции для customerorder:
    - если article -> bundle, разворачиваем через /entity/bundle/{id}/components
    - если обычный товар/вариант, одна позиция
    Цена нам сейчас не критична -> ставим 0 (у тебя цены в товарах есть).
    """
    ass = ms.find_assortment_by_article(article)
    if not ass:
        print({"action": "skip_ms_not_found_by_article", "article": article})
        return []

    t = ((ass.get("meta") or {}).get("type")) or ""
    if t == "bundle":
        rows = ms.get_bundle_components(ass["id"])
        if not rows:
            print({"action": "skip_bundle_no_components", "article": article, "bundle_id": ass.get("id")})
            return []

        out: List[Dict[str, Any]] = []
        for r in rows:
            comp_qty = float(r.get("quantity") or 0)
            if comp_qty <= 0:
                continue
            a = r.get("assortment") or {}
            meta = _ms_meta_from_assortment_dict(a)
            out.append(
                {
                    "assortment": {"meta": meta},
                    "quantity": qty * comp_qty,
                    "price": 0,
                }
            )
        return out

    # обычный товар
    return [
        {
            "assortment": {"meta": (ass.get("meta") or {})},
            "quantity": qty,
            "price": 0,
        }
    ]


def sync() -> None:
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    processed_orders = 0

    for cab in cfg.cabinets:
        oz = OzonFboClient(cab.client_id, cab.api_key)

        for state in SYNC_STATES:
            for order_id in oz.iter_supply_order_ids(state=state, limit=100):
                # исключения
                if order_id in cfg.fbo_exclude_order_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                detail = (oz.get_supply_orders([order_id]).get("orders") or [None])[0]
                if not detail:
                    print({"action": "skip_no_detail", "order_id": order_id})
                    continue

                # отменённые поставки полностью игнорируем
                if detail.get("state") == "CANCELLED":
                    print({"action": "skip_cancelled", "order_id": order_id})
                    continue

                order_number = str(detail.get("order_number") or "").strip()
                if not order_number:
                    print({"action": "skip_no_order_number", "order_id": order_id})
                    continue

                shipment_dt = _parse_timeslot_from(detail)

                # фильтр по planned_from (date)
                if cfg.fbo_planned_from and shipment_dt.date() < cfg.fbo_planned_from:
                    continue

                supplies = detail.get("supplies") or []
                if not supplies:
                    print({"action": "skip_no_supplies", "order_number": order_number, "order_id": order_id})
                    continue

                supply0 = supplies[0]
                bundle_id = supply0.get("bundle_id")
                warehouse_name = (((supply0.get("storage_warehouse") or {}).get("name")) or "").strip()

                if not bundle_id:
                    print({"action": "skip_no_bundle_id", "order_number": order_number, "order_id": order_id})
                    continue

                bundle = oz.get_bundle_items([bundle_id], limit=100)
                items = bundle.get("items") or []
                if not items:
                    print({"action": "skip_empty_bundle_items", "order_number": order_number, "bundle_id": bundle_id})
                    continue

                # externalCode железобетонно
                ext = fbo_external_code(order_number)

                # правило: если уже есть отгрузка (demand) — НИЧЕГО НЕ ОБНОВЛЯЕМ
                existing_demand = dedup_demands_by_external(ms, ext, dry_run=cfg.fbo_dry_run)
                if existing_demand:
                    print({"action": "skip_has_demand", "order_number": order_number, "demand_id": existing_demand.get("id")})
                    continue

                # позиции заказа (с разворотом комплектов)
                positions: List[Dict[str, Any]] = []
                for it in items:
                    article = str(it.get("offer_id") or "").strip()
                    qty = float(it.get("quantity") or 0)
                    if not article or qty <= 0:
                        continue
                    positions.extend(_expand_ms_article_to_positions(ms, article, qty))

                if not positions:
                    print({"action": "skip_no_positions", "order_number": order_number})
                    continue

                description = f"{order_number} - {warehouse_name}".strip(" -")

                draft = CustomerOrderDraft(
                    name=order_number,
                    organization_id=cfg.ms_organization_id,
                    agent_id=cfg.ms_agent_id,
                    state_id=cfg.ms_state_fbo_id,
                    saleschannel_id=cab.ms_saleschannel_id,
                    shipment_planned_at=shipment_dt,
                    description=description,
                    store_id=MOVE_TARGET_STORE_ID,
                )

                order_payload = build_customerorder_payload(draft, positions)

                # 1) order dedup + create/update
                print(ensure_customerorder(ms, order_payload, dry_run=cfg.fbo_dry_run))

                # 2) order_id в МС (по externalCode)
                order_rows = find_customerorders_by_external(ms, ext, limit=1)
                order_id_ms = order_rows[0]["id"] if order_rows else None

                if not order_id_ms:
                    # в dry-run может не быть создано, но если order не найден — дальше бессмысленно
                    print({"action": "skip_no_ms_order_id", "order_number": order_number, "externalCode": ext})
                    continue

                # 3) MOVE 1:1 (по externalCode) — обновляем только позиции
                mv0 = dedup_moves_by_external(ms, ext, dry_run=cfg.fbo_dry_run)
                move_positions = build_move_positions_from_order_positions(order_payload["positions"])

                if not mv0:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_create", "order_number": order_number, "positions": len(move_positions)})
                        move_id = None
                    else:
                        mv = create_move(
                            ms,
                            name=order_number,
                            external_code=ext,
                            organization_id=cfg.ms_organization_id,
                            state_id=cfg.ms_state_fbo_id,  # статус move у тебя отдельный? если да — поменяй на нужный
                            source_store_id=MOVE_SOURCE_STORE_ID,
                            target_store_id=MOVE_TARGET_STORE_ID,
                            description=description,
                            customerorder_id=order_id_ms,
                            positions=move_positions,
                        )
                        move_id = mv.get("id")
                        print({"action": "move_created", "id": move_id, "order_number": order_number})
                else:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_update", "id": mv0["id"], "order_number": order_number})
                        move_id = mv0["id"]
                    else:
                        update_move_positions_only(ms, mv0["id"], move_positions)
                        move_id = mv0["id"]
                        print({"action": "move_updated", "id": move_id, "order_number": order_number})

                if (not cfg.fbo_dry_run) and move_id:
                    print(try_apply_move(ms, move_id))

                # 4) DEMAND создаём только если статус позволяет
                if state not in DEMAND_OZON_STATES:
                    processed_orders += 1
                    continue

                demand_positions = build_demand_positions_from_order_positions(order_payload["positions"])

                if cfg.fbo_dry_run:
                    print({"action": "dry_run_demand_create", "order_number": order_number, "positions": len(demand_positions)})
                    processed_orders += 1
                    continue

                dm = create_demand(
                    ms,
                    name=order_number,
                    external_code=ext,
                    organization_id=cfg.ms_organization_id,
                    agent_id=cfg.ms_agent_id,
                    state_id=cfg.ms_state_fbo_id,  # статус отгрузки у тебя отдельный? если да — поменяй на нужный
                    store_id=MOVE_TARGET_STORE_ID,
                    description=description,
                    customerorder_id=order_id_ms,
                    positions=demand_positions,
                )
                demand_id = dm.get("id")
                print({"action": "demand_created", "id": demand_id, "order_number": order_number})
                if demand_id:
                    print(try_apply_demand(ms, demand_id))

                processed_orders += 1

    print({"action": "done", "processed_orders": processed_orders})


if __name__ == "__main__":
    sync()
