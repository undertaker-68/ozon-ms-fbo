from __future__ import annotations

import os
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import ensure_customerorder, find_customerorders_by_external
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
    update_demand_positions_only,
    build_demand_positions_from_order_positions,
    try_apply_demand,
)

# =========================
# OZON STATES
# =========================
READY_TO_SUPPLY = 2
ACCEPTED_AT_SUPPLY_WAREHOUSE = 3
IN_TRANSIT = 4
ACCEPTANCE_AT_STORAGE_WAREHOUSE = 5
COMPLETED = 8
CANCELLED = 10

SYNC_STATES = (
    READY_TO_SUPPLY,
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
)

DEMAND_OZON_STATES = {
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
}

# =========================
# MOYSKLAD CONSTANTS (как ты задавал ранее)
# =========================
ORGANIZATION_ID = "12d36dcd-8b6c-11e9-9109-f8fc00176e21"
STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"  # FBO склад
AGENT_ID = "f61bfcf9-2d74-11ec-0a80-04c700041e03"

ORDER_STATE_ID = "921c872f-d54e-11ef-0a80-1823001350aa"  # FBO state для customerorder

MOVE_STATE_ID = "b0d2c89d-5c7c-11ef-0a80-0cd4001f5885"
MOVE_SOURCE_STORE_ID = "7cdb9b20-9910-11ec-0a80-08670002d998"
MOVE_TARGET_STORE_ID = STORE_ID

DEMAND_STATE_ID = "b543e330-44e4-11f0-0a80-0da5002260ab"

SALES_CHANNEL_BY_CABINET = {
    0: "fede2826-9fd0-11ee-0a80-0641000f3d25",
    1: "ff2827b8-9fd0-11ee-0a80-0641000f3d31",
}


def _ext_order(order_number: str) -> str:
    return f"OZON_FBO:{order_number}"


def _ext_move(order_id: int) -> str:
    return f"OZON_FBO_MOVE:{order_id}"


def _ext_demand(order_id: int) -> str:
    return f"OZON_FBO_DEMAND:{order_id}"


def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    # часто бывает Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _planned_from_date() -> date:
    """
    По ТЗ: с 03.12.25 включительно.
    Можно переопределить env'ом FBO_PLANNED_FROM (YYYY-MM-DD).
    """
    v = os.getenv("FBO_PLANNED_FROM", "").strip()
    if v:
        try:
            return datetime.fromisoformat(v).date()
        except Exception:
            pass
    return date(2025, 12, 3)


def _exclude_order_ids() -> set[int]:
    v = os.getenv("FBO_EXCLUDE_ORDER_IDS", "").strip()
    if not v:
        return set()
    out: set[int] = set()
    for part in v.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            continue
    return out


def _extract_timeslot_dt(order: Dict[str, Any]) -> Optional[datetime]:
    """
    Берём дату таймслота поставки.
    В данных Ozon это обычно: order["supplies"][0]["timeslot"]["from"]
    """
    supplies = order.get("supplies") or []
    if not supplies:
        return None
    ts = (supplies[0].get("timeslot") or {})
    return _parse_iso_dt(ts.get("from"))


def _extract_warehouse_name(order: Dict[str, Any]) -> str:
    supplies = order.get("supplies") or []
    if not supplies:
        return ""
    wh = supplies[0].get("warehouse_name")
    return str(wh) if wh else ""


def _ozon_items_for_supply(oz: OzonFboClient, order: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    Возвращает список (offer_id/article, qty) из Ozon bundle.
    """
    supplies = order.get("supplies") or []
    if not supplies:
        return []
    bundle_id = supplies[0].get("bundle_id")
    if not bundle_id:
        return []

    b = oz.get_bundle_items([bundle_id], limit=100)
    items = b.get("items") or []
    out: List[Tuple[str, float]] = []
    for it in items:
        offer_id = it.get("offer_id")
        qty = it.get("quantity")
        if offer_id is None or qty is None:
            continue
        out.append((str(offer_id), float(qty)))
    return out


def _expand_to_ms_positions(ms: MoySkladClient, oz_items: List[Tuple[str, float]]) -> List[Dict[str, Any]]:
    """
    Для каждого offer_id:
    - ищем ассортимент по article
    - если product: берём его meta и salePrice
    - если bundle: берём компоненты и разворачиваем в product строки
    """
    out: List[Dict[str, Any]] = []

    for article, qty in oz_items:
        ass = ms.find_assortment_by_article(article)
        if not ass:
            print({"action": "skip_article_not_found_in_ms", "article": article})
            continue

        atype = ((ass.get("meta") or {}).get("type")) or ass.get("type")
        if atype == "bundle":
            bundle_id = ass.get("id")
            if not bundle_id:
                print({"action": "skip_bundle_no_id", "article": article})
                continue

            rows = ms.get_bundle_components(bundle_id)
            if not rows:
                print({"action": "skip_bundle_no_components", "article": article})
                continue

            for r in rows:
                meta = ((r.get("assortment") or {}).get("meta")) or {}
                href = meta.get("href")
                cqty = r.get("quantity")
                if not href or not cqty:
                    continue

                prod = ms.get_by_href(href)
                price = ms.get_sale_price(prod)

                out.append(
                    {
                        "assortment": {"meta": meta},
                        "quantity": float(qty) * float(cqty),
                        "price": int(price),
                    }
                )
        else:
            meta = (ass.get("meta") or {})
            href = meta.get("href")
            if not href:
                print({"action": "skip_assortment_no_href", "article": article})
                continue
            prod = ms.get_by_href(href)
            price = ms.get_sale_price(prod)

            out.append(
                {
                    "assortment": {"meta": meta},
                    "quantity": float(qty),
                    "price": int(price),
                }
            )

    # склеиваем одинаковые товары (по href), суммируем quantity
    merged: Dict[str, Dict[str, Any]] = {}
    for p in out:
        href = (((p.get("assortment") or {}).get("meta") or {}).get("href")) or ""
        if not href:
            continue
        if href not in merged:
            merged[href] = p
        else:
            merged[href]["quantity"] = float(merged[href].get("quantity", 0)) + float(p.get("quantity", 0))
            # price оставляем как есть (в МС это обычно ок)

    return list(merged.values())


def sync() -> int:
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    dry_run = bool(cfg.fbo_dry_run) or os.getenv("FBO_DRY_RUN", "").strip() in ("1", "true", "yes", "on")
    planned_from = _planned_from_date()
    excluded = _exclude_order_ids()

    processed = 0
    created_orders = 0
    updated_orders = 0
    skipped_cancelled = 0
    skipped_excluded = 0
    skipped_by_date = 0
    skipped_no_positions = 0

    for cab_index, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel_id = SALES_CHANNEL_BY_CABINET.get(cab_index, SALES_CHANNEL_BY_CABINET[0])

        for state in SYNC_STATES:
            # отменённые не трогаем вообще (на всякий)
            if state == CANCELLED:
                continue

            for order_id in oz.iter_supply_order_ids(state=state, limit=100):
                if order_id in excluded:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    skipped_excluded += 1
                    continue

                # детали поставки
                detail = oz.get_supply_orders([order_id])
                orders = detail.get("orders") or []
                if not orders:
                    continue
                o = orders[0]

                order_number = str(o.get("order_number") or order_id)
                wh_name = _extract_warehouse_name(o)

                # фильтр по таймслоту
                ship_dt = _extract_timeslot_dt(o)
                if ship_dt is None:
                    # без таймслота — пропускаем
                    print({"action": "skip_no_timeslot", "order_number": order_number, "order_id": order_id})
                    continue

                if ship_dt.date() < planned_from:
                    skipped_by_date += 1
                    continue

                # если внезапно cancelled в деталях
                if int(o.get("state", state)) == CANCELLED:
                    skipped_cancelled += 1
                    continue

                comment = f"{order_number} - {wh_name}".strip(" -")
                delivery_planned = ship_dt.strftime("%Y-%m-%d %H:%M:%S.000")

                oz_items = _ozon_items_for_supply(oz, o)
                ms_positions = _expand_to_ms_positions(ms, oz_items)

                if not ms_positions:
                    print({"action": "skip_no_positions_after_expand", "order_number": order_number, "order_id": order_id})
                    skipped_no_positions += 1
                    continue

                # externalCode для заказа
                ext_order = _ext_order(order_number)

                # 1) customerorder dedup + create/update
                payload_order: Dict[str, Any] = {
                    "name": order_number,
                    "externalCode": ext_order,
                    "organization": ms.meta("organization", ORGANIZATION_ID),
                    "agent": ms.meta("counterparty", AGENT_ID),
                    "state": ms.meta("state", ORDER_STATE_ID),
                    "salesChannel": ms.meta("saleschannel", sales_channel_id),
                    "description": comment,
                    "store": ms.meta("store", STORE_ID),
                    "deliveryPlannedMoment": delivery_planned,
                    "positions": ms_positions,
                }

                # правило: если уже есть demand — заказ НЕ обновляем
                ext_dem = _ext_demand(order_id)
                existing_dem = dedup_demands_by_external(ms, ext_dem, dry_run=dry_run)
                if existing_dem:
                    # но заказ должен существовать (если руками удаляли — восстановим)
                    rows = find_customerorders_by_external(ms, ext_order)
                    if not rows:
                        r = ensure_customerorder(ms, payload_order, dry_run=dry_run)
                        print(r)
                        if r.get("action") == "created":
                            created_orders += 1
                    else:
                        print({"action": "skip_order_update_because_demand_exists", "order_number": order_number, "demand_id": existing_dem.get("id")})
                else:
                    r = ensure_customerorder(ms, payload_order, dry_run=dry_run)
                    print(r)
                    if r.get("action") == "created":
                        created_orders += 1
                    if r.get("action") == "updated":
                        updated_orders += 1

                # получаем order id в МС (нужно для связи move/demand)
                order_rows = find_customerorders_by_external(ms, ext_order)
                if not order_rows:
                    # в dry_run может быть пусто — тогда пропускаем создание связанных документов
                    if dry_run:
                        processed += 1
                        continue
                    # иначе это ошибка данных
                    print({"action": "error_order_not_found_after_ensure", "order_number": order_number, "externalCode": ext_order})
                    continue

                order_ms = order_rows[-1]
                order_ms_id = order_ms.get("id")
                if not order_ms_id:
                    if dry_run:
                        processed += 1
                        continue
                    print({"action": "error_order_missing_id", "order_number": order_number})
                    continue

                # 2) MOVE: 1 заказ = 1 перемещение, dedup по external
                ext_mv = _ext_move(order_id)
                keep_mv = dedup_moves_by_external(ms, ext_mv, dry_run=dry_run)

                move_positions = build_move_positions_from_order_positions(ms_positions)

                payload_move: Dict[str, Any] = {
                    "name": order_number,
                    "externalCode": ext_mv,
                    "organization": ms.meta("organization", ORGANIZATION_ID),
                    "state": ms.meta("state", MOVE_STATE_ID),
                    "sourceStore": ms.meta("store", MOVE_SOURCE_STORE_ID),
                    "targetStore": ms.meta("store", MOVE_TARGET_STORE_ID),
                    "description": comment,
                    "customerOrder": ms.meta("customerorder", order_ms_id),
                    "positions": move_positions,
                    "applicable": False,  # создаём не проведённым
                }

                if dry_run:
                    print({"action": "dry_run_move_create" if not keep_mv else "dry_run_move_update", "externalCode": ext_mv, "positions": len(move_positions)})
                else:
                    if keep_mv:
                        update_move_positions_only(ms, keep_mv["id"], move_positions)
                        move_id = keep_mv["id"]
                        print({"action": "move_updated", "id": move_id, "name": order_number})
                    else:
                        mv = create_move(ms, payload_move)
                        move_id = mv.get("id")
                        print({"action": "move_created", "id": move_id, "name": order_number})

                    if move_id:
                        print(try_apply_move(ms, move_id))

                # 3) DEMAND: только для нужных статусов (3/4/5/8)
                if state in DEMAND_OZON_STATES:
                    demand_positions = build_demand_positions_from_order_positions(ms_positions)

                    payload_dem: Dict[str, Any] = {
                        "name": order_number,
                        "externalCode": ext_dem,
                        "organization": ms.meta("organization", ORGANIZATION_ID),
                        "agent": ms.meta("counterparty", AGENT_ID),
                        "store": ms.meta("store", STORE_ID),
                        "state": ms.meta("state", DEMAND_STATE_ID),
                        "description": comment,
                        "customerOrder": ms.meta("customerorder", order_ms_id),
                        "positions": demand_positions,
                        "applicable": False,
                    }

                    keep_dem = dedup_demands_by_external(ms, ext_dem, dry_run=dry_run)

                    if dry_run:
                        print({"action": "dry_run_demand_create" if not keep_dem else "dry_run_demand_exists", "externalCode": ext_dem, "positions": len(demand_positions)})
                    else:
                        if keep_dem:
                            # если отгрузка уже есть — НЕ обновляем (как требование)
                            print({"action": "skip_demand_exists", "id": keep_dem.get("id"), "externalCode": ext_dem})
                        else:
                            dem = create_demand(ms, payload_dem)
                            demand_id = dem.get("id")
                            print({"action": "demand_created", "id": demand_id, "name": order_number})
                            if demand_id:
                                print(try_apply_demand(ms, demand_id))

                processed += 1

    print(
        {
            "action": "sync_done",
            "dry_run": dry_run,
            "planned_from": planned_from.isoformat(),
            "processed": processed,
            "created_orders": created_orders,
            "updated_orders": updated_orders,
            "skipped_cancelled": skipped_cancelled,
            "skipped_excluded": skipped_excluded,
            "skipped_by_date": skipped_by_date,
            "skipped_no_positions": skipped_no_positions,
        }
    )
    return processed


if __name__ == "__main__":
    sync()
